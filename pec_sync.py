"""
Ceraldi PEC Sync v2
Legge la PEC Aruba via IMAP, parsea le fatture XML SDI,
salva l'indice JSON su GitHub (nel repo esistente).
L'app legge il JSON direttamente da GitHub (raw URL pubblico).
"""

import imaplib
import email
import os
import json
import time
import logging
import zipfile
import io
import urllib.request
import urllib.error
from datetime import datetime, timezone
from xml.etree import ElementTree as ET
import schedule

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("ceraldi-pec")

# ── Config da env ─────────────────────────────────────────────────────────────
PEC_HOST    = os.environ.get("PEC_HOST", "imaps.pec.aruba.it")
PEC_PORT    = int(os.environ.get("PEC_PORT", "993"))
PEC_USER    = os.environ["PEC_USER"]
PEC_PASS    = os.environ["PEC_PASS"]
PEC_MAILBOX = os.environ.get("PEC_MAILBOX", "INBOX")

# GitHub: repo dove salvare l'indice
GH_TOKEN    = os.environ["GH_TOKEN"]       # Personal Access Token GitHub
GH_REPO     = os.environ["GH_REPO"]        # es. valerioceraldi-hub/ceraldi-pec-sync
GH_BRANCH   = os.environ.get("GH_BRANCH", "main")
GH_INDEX_PATH = "ceraldi_fatture_index.json"
GH_PROCESSED_PATH = "processed_ids.json"

SYNC_INTERVAL_HOURS = int(os.environ.get("SYNC_INTERVAL_HOURS", "24"))

# ── GitHub API ────────────────────────────────────────────────────────────────
GH_API = "https://api.github.com"

def gh_get(path: str) -> dict:
    req = urllib.request.Request(
        f"{GH_API}{path}",
        headers={
            "Authorization": f"token {GH_TOKEN}",
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "ceraldi-pec-sync"
        }
    )
    try:
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return {}
        raise

def gh_put(path: str, content: bytes, message: str, sha: str = None):
    import base64
    body = {
        "message": message,
        "content": base64.b64encode(content).decode(),
        "branch": GH_BRANCH,
    }
    if sha:
        body["sha"] = sha
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        f"{GH_API}{path}",
        data=data,
        method="PUT",
        headers={
            "Authorization": f"token {GH_TOKEN}",
            "Accept": "application/vnd.github.v3+json",
            "Content-Type": "application/json",
            "User-Agent": "ceraldi-pec-sync"
        }
    )
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read().decode())

def read_gh_json(filepath: str, default):
    """Legge un file JSON dal repo GitHub."""
    data = gh_get(f"/repos/{GH_REPO}/contents/{filepath}?ref={GH_BRANCH}")
    if not data or "content" not in data:
        return default, None
    import base64
    content = base64.b64decode(data["content"]).decode()
    return json.loads(content), data.get("sha")

def write_gh_json(filepath: str, obj, message: str, sha: str = None):
    """Scrive/aggiorna un file JSON nel repo GitHub."""
    content = json.dumps(obj, ensure_ascii=False, indent=2).encode()
    result = gh_put(f"/repos/{GH_REPO}/contents/{filepath}", content, message, sha)
    log.info(f"  → GitHub: {filepath} aggiornato")
    return result.get("content", {}).get("sha")

# ── SDI XML Parser ────────────────────────────────────────────────────────────
def parse_sdi_xml(xml_bytes: bytes) -> dict | None:
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as e:
        log.warning(f"  XML parse error: {e}")
        return None

    tag = root.tag
    ns = ""
    if tag.startswith("{"):
        ns_uri = tag[1: tag.index("}")]
        ns = f"{{{ns_uri}}}"

    def tx(path):
        el = root.find(path.replace("./", f"./{ns}").replace("/", f"/{ns}"))
        if el is None:
            el = root.find(path)
        return el.text.strip() if el is not None and el.text else ""

    def find_all(path):
        els = root.findall(path.replace("./", f"./{ns}").replace("/", f"/{ns}"))
        if not els:
            els = root.findall(path)
        return els

    denom   = tx("./FatturaElettronicaHeader/CedentePrestatore/DatiAnagrafici/Anagrafica/Denominazione")
    cognome = tx("./FatturaElettronicaHeader/CedentePrestatore/DatiAnagrafici/Anagrafica/Cognome")
    nome_f  = tx("./FatturaElettronicaHeader/CedentePrestatore/DatiAnagrafici/Anagrafica/Nome")
    fornitore = denom or f"{cognome} {nome_f}".strip()

    numero   = tx("./FatturaElettronicaBody/DatiGenerali/DatiGeneraliDocumento/Numero")
    data_raw = tx("./FatturaElettronicaBody/DatiGenerali/DatiGeneraliDocumento/Data")
    tipo_doc = tx("./FatturaElettronicaBody/DatiGenerali/DatiGeneraliDocumento/TipoDocumento")
    importo  = tx("./FatturaElettronicaBody/DatiGenerali/DatiGeneraliDocumento/ImportoTotaleDocumento")

    if not importo:
        totali = find_all("./FatturaElettronicaBody/DatiBeniServizi/DatiRiepilogo/ImponibileImporto")
        imp_sum = sum(float(t.text.replace(",", ".")) for t in totali if t.text)
        iva_els = find_all("./FatturaElettronicaBody/DatiBeniServizi/DatiRiepilogo/Imposta")
        iva_sum = sum(float(t.text.replace(",", ".")) for t in iva_els if t.text)
        importo = str(round(imp_sum + iva_sum, 2)) if imp_sum else ""

    scadenza = tx("./FatturaElettronicaBody/DatiPagamento/DettaglioPagamento/DataScadenzaPagamento")
    modalita_map = {
        "MP01": "contanti", "MP02": "assegno", "MP05": "bonifico",
        "MP08": "carta",    "MP10": "rid",     "MP12": "riba",
    }
    mod_raw   = tx("./FatturaElettronicaBody/DatiPagamento/DettaglioPagamento/ModalitaPagamento")
    pagamento = modalita_map.get(mod_raw, "")
    iban      = tx("./FatturaElettronicaBody/DatiPagamento/DettaglioPagamento/IBAN")
    tipo      = "ddt" if tipo_doc in ("TD24", "TD25", "TD26") else "fattura"
    data      = data_raw[:10] if data_raw else ""

    if not fornitore or not numero:
        log.warning("  XML incompleto: fornitore o numero mancante")
        return None

    imp_float = 0.0
    try:
        imp_float = float(str(importo).replace(",", "."))
    except (ValueError, TypeError):
        pass

    return {
        "id":         f"pec_{int(time.time()*1000)}_{numero}",
        "tipo":       tipo,
        "fornitore":  fornitore,
        "numero":     numero,
        "data":       data,
        "importo":    imp_float,
        "scadenza":   scadenza[:10] if scadenza else "",
        "pagamento":  pagamento,
        "bonIban":    iban,
        "stato":      "da_pagare",
        "note":       f"Importato da PEC ({datetime.now(timezone.utc).strftime('%d/%m/%Y')})",
        "rate":       [],
        "source":     "pec",
        "importedAt": datetime.now(timezone.utc).isoformat(),
    }

# ── P7M / ZIP ─────────────────────────────────────────────────────────────────
def _extract_from_p7m(p7m_bytes: bytes) -> bytes | None:
    """
    Estrae XML da un file .p7m (CMS/CAdES).
    """
    # Metodo 1: cerca direttamente marker XML nel payload
    for marker in (b"<?xml", b"<FatturaElettronica", b"<p:FatturaElettronica",
                   b"<ns0:FatturaElettronica", b"<ns2:FatturaElettronica"):
        idx = p7m_bytes.find(marker)
        if idx != -1:
            content = p7m_bytes[idx:]
            for end_marker in (b"</FatturaElettronica>", b"</p:FatturaElettronica>",
                               b"</ns0:FatturaElettronica>", b"</ns2:FatturaElettronica>"):
                end_idx = content.rfind(end_marker)
                if end_idx != -1:
                    return content[:end_idx + len(end_marker)]
            return content

    # Metodo 2: struttura ASN.1 — cerca OCTET STRING che contiene XML
    try:
        i = 0
        while i < len(p7m_bytes) - 10:
            if p7m_bytes[i] == 0x04:  # OCTET STRING
                l_byte = p7m_bytes[i+1]
                if l_byte < 0x80:
                    length = l_byte
                    data_start = i + 2
                elif l_byte == 0x81:
                    length = p7m_bytes[i+2]
                    data_start = i + 3
                elif l_byte == 0x82:
                    length = (p7m_bytes[i+2] << 8) | p7m_bytes[i+3]
                    data_start = i + 4
                elif l_byte == 0x83:
                    length = (p7m_bytes[i+2] << 16) | (p7m_bytes[i+3] << 8) | p7m_bytes[i+4]
                    data_start = i + 5
                else:
                    i += 1
                    continue
                if data_start + 5 < len(p7m_bytes):
                    chunk = p7m_bytes[data_start:data_start+10]
                    if any(chunk.startswith(m) for m in
                           (b"<?xml", b"<Fattura", b"<p:Fattura", b"<ns")):
                        return p7m_bytes[data_start:data_start+length]
            i += 1
    except Exception:
        pass

    return None

def _extract_xml_from_zip(zip_bytes: bytes):
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

# ── IMAP ──────────────────────────────────────────────────────────────────────
def fetch_pec_messages(processed_ids: list) -> list[dict]:
    results = []
    log.info(f"Connessione IMAP a {PEC_HOST}:{PEC_PORT}…")

    with imaplib.IMAP4_SSL(PEC_HOST, PEC_PORT) as imap:
        imap.login(PEC_USER, PEC_PASS)

        # Seleziona la cartella — prova varie codifiche
        status, msgs = None, None
        for mailbox_try in [f'"{PEC_MAILBOX}"', PEC_MAILBOX,
                            PEC_MAILBOX.encode("utf-7").decode(), "INBOX"]:
            try:
                status, msgs = imap.select(mailbox_try)
                if status == "OK":
                    log.info(f"  Cartella aperta: {mailbox_try} ({msgs[0].decode()} email)")
                    break
            except Exception:
                continue

        if status != "OK":
            # Mostra cartelle disponibili per debug
            _, folders = imap.list()
            log.error("  Cartelle disponibili:")
            for f in (folders or []):
                log.error(f"    {f}")
            return results

        # Prendi tutte le email
        _, data = imap.search(None, "ALL")
        all_uids_list = data[0].split()
        log.info(f"  Email trovate: {len(all_uids_list)}")
        all_uids = set(u.decode() for u in all_uids_list)

        for uid_str in sorted(all_uids, reverse=True):
            if uid_str in processed_ids:
                continue

            try:
                _, msg_data = imap.fetch(uid_str.encode(), "(RFC822)")
                if not msg_data or not msg_data[0]:
                    processed_ids.append(uid_str)
                    continue
                raw = msg_data[0][1]
                msg = email.message_from_bytes(raw)
            except Exception as e:
                log.warning(f"  Errore fetch uid {uid_str}: {e}")
                processed_ids.append(uid_str)
                continue

            found_xml = False
            for part in msg.walk():
                ct = part.get_content_type()
                fn = part.get_filename() or ""
                fn_lower = fn.lower()
                xml_bytes = None
                out_filename = fn

                if fn_lower.endswith(".xml"):
                    xml_bytes = part.get_payload(decode=True)

                elif fn_lower.endswith(".xml.p7m") or fn_lower.endswith(".p7m"):
                    p7m = part.get_payload(decode=True)
                    if p7m:
                        xml_bytes = _extract_from_p7m(p7m)
                        if not xml_bytes:
                            # Prova a usare il payload direttamente
                            xml_bytes = p7m
                        out_filename = fn.replace(".p7m", "")

                elif fn_lower.endswith(".zip"):
                    zdata = part.get_payload(decode=True)
                    if zdata:
                        xml_bytes, out_filename = _extract_xml_from_zip(zdata)

                elif ct in ("text/xml", "application/xml",
                            "application/pkcs7-mime", "application/x-pkcs7-mime"):
                    payload = part.get_payload(decode=True)
                    if payload:
                        xml_bytes = _extract_from_p7m(payload) or payload

                if xml_bytes and len(xml_bytes) > 100:
                    log.info(f"  → Allegato: {out_filename} (uid {uid_str}, {len(xml_bytes)} bytes)")
                    results.append({
                        "uid": uid_str,
                        "filename": out_filename or f"fattura_{uid_str}.xml",
                        "xml_bytes": xml_bytes,
                    })
                    found_xml = True
                    break  # una fattura per email

            if not found_xml:
                processed_ids.append(uid_str)

        imap.logout()

    return results

# ── Sync principale ───────────────────────────────────────────────────────────
def sync():
    log.info("═══ Avvio sync PEC → GitHub ═══")

    index, index_sha = read_gh_json(GH_INDEX_PATH, {"fatture": [], "lastSync": ""})
    processed, processed_sha = read_gh_json(GH_PROCESSED_PATH, [])

    messages = fetch_pec_messages(processed)
    log.info(f"Nuove email con XML: {len(messages)}")

    new_count = 0
    for msg in messages:
        xml_bytes = msg["xml_bytes"]
        filename  = msg["filename"]

        fattura = parse_sdi_xml(xml_bytes)
        if not fattura:
            log.warning(f"  Skip {filename}: parsing fallito")
            processed.append(msg["uid"])
            continue

        chiave = f"{fattura['fornitore']}|{fattura['numero']}"
        if any(f"{f.get('fornitore')}|{f.get('numero')}" == chiave for f in index.get("fatture", [])):
            log.info(f"  Duplicato: {chiave} — skip")
            processed.append(msg["uid"])
            continue

        index.setdefault("fatture", []).append(fattura)
        processed.append(msg["uid"])
        new_count += 1
        log.info(f"  ✓ {fattura['fornitore']} n.{fattura['numero']} €{fattura['importo']}")

    index["lastSync"] = datetime.now(timezone.utc).isoformat()
    index["newCount"] = new_count

    write_gh_json(GH_INDEX_PATH, index,
                  f"Sync PEC: {new_count} nuove fatture ({datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')})",
                  index_sha)
    write_gh_json(GH_PROCESSED_PATH, processed,
                  "Aggiorna processed_ids",
                  processed_sha)

    log.info(f"═══ Sync completata: {new_count} nuove fatture ═══\n")

# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    sync()
    schedule.every(SYNC_INTERVAL_HOURS).hours.do(sync)
    log.info(f"Scheduler: sync ogni {SYNC_INTERVAL_HOURS}h")
    while True:
        schedule.run_pending()
        time.sleep(60)
