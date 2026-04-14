"""
Ceraldi PEC Sync
Legge la PEC Aruba via IMAP, parsea le fatture XML SDI,
carica su Google Drive e aggiorna un file JSON indice.
"""

import imaplib
import email
import os
import json
import time
import logging
import re
import base64
import zipfile
import io
from datetime import datetime, timezone
from xml.etree import ElementTree as ET

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
import schedule

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("ceraldi-pec")

# ── Config da env ─────────────────────────────────────────────────────────────
PEC_HOST     = os.environ.get("PEC_HOST", "imaps.pec.aruba.it")
PEC_PORT     = int(os.environ.get("PEC_PORT", "993"))
PEC_USER     = os.environ["PEC_USER"]          # es. tuanome@pec.it
PEC_PASS     = os.environ["PEC_PASS"]
PEC_MAILBOX  = os.environ.get("PEC_MAILBOX", "INBOX")

DRIVE_FOLDER_ID         = os.environ["DRIVE_FOLDER_ID"]   # ID cartella Google Drive
GOOGLE_SERVICE_ACCOUNT  = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]  # JSON inline

INDEX_FILE   = "ceraldi_fatture_index.json"
PROCESSED_FILE = "processed_ids.json"

SYNC_INTERVAL_HOURS = int(os.environ.get("SYNC_INTERVAL_HOURS", "24"))

# ── Google Drive ──────────────────────────────────────────────────────────────
def get_drive_service():
    sa_info = json.loads(GOOGLE_SERVICE_ACCOUNT)
    creds = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=["https://www.googleapis.com/auth/drive"]
    )
    return build("drive", "v3", credentials=creds)


def upload_to_drive(service, filename: str, content: bytes, mime: str) -> str:
    """Carica file su Drive e restituisce il file ID."""
    meta = {"name": filename, "parents": [DRIVE_FOLDER_ID]}
    media = MediaIoBaseUpload(io.BytesIO(content), mimetype=mime)
    f = service.files().create(body=meta, media_body=media, fields="id,webViewLink").execute()
    log.info(f"  → Drive: {filename} ({f['id']})")
    return f["id"], f.get("webViewLink", "")


def read_drive_json(service, filename: str) -> dict | list:
    """Legge un file JSON da Drive. Restituisce {} se non esiste."""
    results = service.files().list(
        q=f"name='{filename}' and '{DRIVE_FOLDER_ID}' in parents and trashed=false",
        fields="files(id)"
    ).execute()
    files = results.get("files", [])
    if not files:
        return {} if filename == INDEX_FILE else []
    fid = files[0]["id"]
    data = service.files().get_media(fileId=fid).execute()
    return json.loads(data.decode())


def write_drive_json(service, filename: str, data):
    """Sovrascrive (o crea) un file JSON su Drive."""
    content = json.dumps(data, ensure_ascii=False, indent=2).encode()
    results = service.files().list(
        q=f"name='{filename}' and '{DRIVE_FOLDER_ID}' in parents and trashed=false",
        fields="files(id)"
    ).execute()
    files = results.get("files", [])
    media = MediaIoBaseUpload(io.BytesIO(content), mimetype="application/json")
    if files:
        service.files().update(fileId=files[0]["id"], media_body=media).execute()
    else:
        meta = {"name": filename, "parents": [DRIVE_FOLDER_ID]}
        service.files().create(body=meta, media_body=media).execute()
    log.info(f"  → JSON aggiornato: {filename}")


# ── SDI XML Parser ────────────────────────────────────────────────────────────
NS = {
    "p": "http://ivaservizi.agenziaentrate.gov.it/docs/xsd/fatture/v1.2",
    "ds": "http://www.w3.org/2000/09/xmldsig#",
}

def _find(root, *paths):
    """Cerca un testo in più xpath alternativi."""
    for path in paths:
        el = root.find(path, NS)
        if el is not None and el.text:
            return el.text.strip()
    return ""


def parse_sdi_xml(xml_bytes: bytes) -> dict | None:
    """
    Parsea un XML FatturaPA / SDI e restituisce un dict
    compatibile con il modello dati di Ceraldi Fatture.
    """
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as e:
        log.warning(f"  XML parse error: {e}")
        return None

    # Namespace dinamico (alcuni file omettono il prefisso)
    ns = ""
    tag = root.tag
    if tag.startswith("{"):
        ns_uri = tag[1: tag.index("}")]
        ns = f"{{{ns_uri}}}"

    def tx(path):
        """Cerca sia con che senza namespace."""
        el = root.find(path.replace("./", f"./{ns}").replace("/", f"/{ns}"))
        if el is None:
            el = root.find(path)
        return el.text.strip() if el is not None and el.text else ""

    def find_all(path):
        els = root.findall(path.replace("./", f"./{ns}").replace("/", f"/{ns}"))
        if not els:
            els = root.findall(path)
        return els

    # Cedente (fornitore)
    denom   = tx("./FatturaElettronicaHeader/CedentePrestatore/DatiAnagrafici/Anagrafica/Denominazione")
    cognome = tx("./FatturaElettronicaHeader/CedentePrestatore/DatiAnagrafici/Anagrafica/Cognome")
    nome    = tx("./FatturaElettronicaHeader/CedentePrestatore/DatiAnagrafici/Anagrafica/Nome")
    fornitore = denom or f"{cognome} {nome}".strip()

    # Dati generali documento
    numero   = tx("./FatturaElettronicaBody/DatiGenerali/DatiGeneraliDocumento/Numero")
    data_raw = tx("./FatturaElettronicaBody/DatiGenerali/DatiGeneraliDocumento/Data")
    tipo_doc = tx("./FatturaElettronicaBody/DatiGenerali/DatiGeneraliDocumento/TipoDocumento")
    importo  = tx("./FatturaElettronicaBody/DatiGenerali/DatiGeneraliDocumento/ImportoTotaleDocumento")

    # Se importo mancante, somma ImponibileImporto delle righe riepilogo
    if not importo:
        totali = find_all("./FatturaElettronicaBody/DatiBeniServizi/DatiRiepilogo/ImponibileImporto")
        imp_sum = sum(float(t.text.replace(",", ".")) for t in totali if t.text)
        iva_els = find_all("./FatturaElettronicaBody/DatiBeniServizi/DatiRiepilogo/Imposta")
        iva_sum = sum(float(t.text.replace(",", ".")) for t in iva_els if t.text)
        importo = str(round(imp_sum + iva_sum, 2)) if imp_sum else ""

    # Scadenza pagamento
    scadenza = tx("./FatturaElettronicaBody/DatiPagamento/DettaglioPagamento/DataScadenzaPagamento")

    # Metodo pagamento
    modalita_map = {
        "MP01": "contanti", "MP02": "assegno", "MP05": "bonifico",
        "MP08": "carta",    "MP10": "rid",     "MP12": "riba",
    }
    mod_raw  = tx("./FatturaElettronicaBody/DatiPagamento/DettaglioPagamento/ModalitaPagamento")
    pagamento = modalita_map.get(mod_raw, "")

    # IBAN
    iban = tx("./FatturaElettronicaBody/DatiPagamento/DettaglioPagamento/IBAN")

    # Tipo documento → tipo app
    tipo = "fattura"
    if tipo_doc in ("TD24", "TD25", "TD26"):  # DDT-related
        tipo = "ddt"

    # Formatta data YYYY-MM-DD → YYYY-MM-DD (già ok)
    data = data_raw[:10] if data_raw else ""

    if not fornitore or not numero:
        log.warning("  XML incompleto: fornitore o numero mancante")
        return None

    imp_float = 0.0
    try:
        imp_float = float(str(importo).replace(",", "."))
    except (ValueError, TypeError):
        pass

    return {
        "id":        f"pec_{int(time.time()*1000)}_{numero}",
        "tipo":      tipo,
        "fornitore": fornitore,
        "numero":    numero,
        "data":      data,
        "importo":   imp_float,
        "scadenza":  scadenza[:10] if scadenza else "",
        "pagamento": pagamento,
        "bonIban":   iban,
        "stato":     "da_pagare",
        "note":      f"Importato automaticamente da PEC ({datetime.now(timezone.utc).strftime('%d/%m/%Y')})",
        "rate":      [],
        "foto":      None,
        "driveId":   "",   # verrà popolato dopo upload
        "driveUrl":  "",
        "source":    "pec",
        "importedAt": datetime.now(timezone.utc).isoformat(),
    }


# ── IMAP ─────────────────────────────────────────────────────────────────────
def fetch_pec_messages(processed_ids: list) -> list[dict]:
    """
    Connette alla PEC, cerca email con allegati XML SDI non ancora processate.
    Restituisce lista di {uid, filename, xml_bytes, raw_xml}.
    """
    results = []
    log.info(f"Connessione IMAP a {PEC_HOST}:{PEC_PORT}…")

    with imaplib.IMAP4_SSL(PEC_HOST, PEC_PORT) as imap:
        imap.login(PEC_USER, PEC_PASS)
        imap.select(PEC_MAILBOX)

        # Cerca email con allegati (tutti, filtriamo dopo)
        _, data = imap.search(None, "ALL")
        uids = data[0].split()
        log.info(f"  Trovate {len(uids)} email in {PEC_MAILBOX}")

        for uid in reversed(uids[-500:]):  # ultime 500
            uid_str = uid.decode()
            if uid_str in processed_ids:
                continue

            _, msg_data = imap.fetch(uid, "(RFC822)")
            raw = msg_data[0][1]
            msg = email.message_from_bytes(raw)

            found_xml = False
            for part in msg.walk():
                ct = part.get_content_type()
                fn = part.get_filename() or ""

                # Allegati XML SDI (anche dentro zip o p7m)
                xml_bytes = None
                out_filename = fn

                if fn.lower().endswith(".xml") or ct in ("text/xml", "application/xml"):
                    xml_bytes = part.get_payload(decode=True)

                elif fn.lower().endswith(".xml.p7m"):
                    # File firmato CAdES — estrai il payload interno
                    p7m = part.get_payload(decode=True)
                    xml_bytes = _extract_from_p7m(p7m)
                    out_filename = fn.replace(".p7m", "")

                elif fn.lower().endswith(".zip"):
                    zdata = part.get_payload(decode=True)
                    xml_bytes, out_filename = _extract_xml_from_zip(zdata)

                if xml_bytes:
                    log.info(f"  → Allegato trovato: {out_filename} (uid {uid_str})")
                    results.append({
                        "uid": uid_str,
                        "filename": out_filename or f"fattura_{uid_str}.xml",
                        "xml_bytes": xml_bytes,
                        "subject": msg.get("Subject", ""),
                        "date": msg.get("Date", ""),
                    })
                    found_xml = True

            if not found_xml:
                # Marca come processata (email senza XML utile)
                processed_ids.append(uid_str)

        imap.logout()

    return results


def _extract_from_p7m(p7m_bytes: bytes) -> bytes | None:
    """
    Estrae il contenuto XML da un file .p7m (CMS/PKCS#7).
    Usa una euristica semplice: cerca il tag <?xml o <Fattura.
    """
    try:
        text = p7m_bytes
        # Cerca inizio XML
        for marker in (b"<?xml", b"<FatturaElettronica", b"<p:FatturaElettronica"):
            idx = text.find(marker)
            if idx != -1:
                return text[idx:]
    except Exception as e:
        log.warning(f"  p7m extract error: {e}")
    return None


def _extract_xml_from_zip(zip_bytes: bytes) -> tuple[bytes | None, str]:
    """Estrae il primo file XML da uno zip."""
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            for name in zf.namelist():
                if name.lower().endswith(".xml") or name.lower().endswith(".xml.p7m"):
                    data = zf.read(name)
                    if name.lower().endswith(".p7m"):
                        data = _extract_from_p7m(data) or data
                    return data, name.replace(".p7m", "")
    except Exception as e:
        log.warning(f"  zip extract error: {e}")
    return None, ""


# ── Sync principale ───────────────────────────────────────────────────────────
def sync():
    log.info("═══ Avvio sync PEC → Drive ═══")
    drive = get_drive_service()

    # Leggi stato precedente
    index: dict = read_drive_json(drive, INDEX_FILE)
    if not isinstance(index, dict):
        index = {"fatture": [], "lastSync": ""}

    processed: list = read_drive_json(drive, PROCESSED_FILE)
    if not isinstance(processed, list):
        processed = []

    # Fetch email PEC
    messages = fetch_pec_messages(processed)
    log.info(f"Nuove email con XML: {len(messages)}")

    new_count = 0
    for msg in messages:
        xml_bytes = msg["xml_bytes"]
        filename  = msg["filename"]

        # Parsea XML
        fattura = parse_sdi_xml(xml_bytes)
        if not fattura:
            log.warning(f"  Skip {filename}: parsing fallito")
            processed.append(msg["uid"])
            continue

        # Controlla duplicati per numero+fornitore
        chiave = f"{fattura['fornitore']}|{fattura['numero']}"
        esistenti = [f for f in index.get("fatture", [])
                     if f"{f.get('fornitore')}|{f.get('numero')}" == chiave]
        if esistenti:
            log.info(f"  Duplicato: {chiave} — skip")
            processed.append(msg["uid"])
            continue

        # Carica XML originale su Drive
        drive_id, drive_url = upload_to_drive(drive, filename, xml_bytes, "application/xml")
        fattura["driveId"]  = drive_id
        fattura["driveUrl"] = drive_url

        # Aggiungi all'indice
        index.setdefault("fatture", []).append(fattura)
        processed.append(msg["uid"])
        new_count += 1
        log.info(f"  ✓ Fattura aggiunta: {fattura['fornitore']} n.{fattura['numero']} €{fattura['importo']}")

    # Aggiorna timestamp
    index["lastSync"] = datetime.now(timezone.utc).isoformat()
    index["newCount"] = new_count

    # Scrivi indice aggiornato su Drive
    write_drive_json(drive, INDEX_FILE, index)
    write_drive_json(drive, PROCESSED_FILE, processed)

    log.info(f"═══ Sync completata: {new_count} nuove fatture ═══\n")


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Prima esecuzione immediata
    sync()

    # Poi ogni N ore
    schedule.every(SYNC_INTERVAL_HOURS).hours.do(sync)
    log.info(f"Scheduler avviato: sync ogni {SYNC_INTERVAL_HOURS}h")

    while True:
        schedule.run_pending()
        time.sleep(60)
