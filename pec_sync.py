import imaplib, email, os, json, time, logging, zipfile, io, urllib.request, base64
from datetime import datetime, timezone
from xml.etree import ElementTree as ET
import schedule

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("ceraldi-pec")

PEC_HOST = os.environ.get("PEC_HOST", "imaps.pec.aruba.it")
PEC_PORT = int(os.environ.get("PEC_PORT", "993"))
PEC_USER = os.environ["PEC_USER"]
PEC_PASS = os.environ["PEC_PASS"]
GH_TOKEN = os.environ["GH_TOKEN"]
GH_REPO  = os.environ["GH_REPO"]
GH_BRANCH = os.environ.get("GH_BRANCH", "main")
SYNC_INTERVAL_HOURS = int(os.environ.get("SYNC_INTERVAL_HOURS", "24"))

def gh_read(path, default):
    req = urllib.request.Request(
        f"https://api.github.com/repos/{GH_REPO}/contents/{path}?ref={GH_BRANCH}",
        headers={"Authorization": f"token {GH_TOKEN}", "Accept": "application/vnd.github.v3+json", "User-Agent": "ceraldi"}
    )
    try:
        with urllib.request.urlopen(req) as r:
            data = json.loads(r.read())
            return json.loads(base64.b64decode(data["content"]).decode()), data.get("sha")
    except:
        return default, None

def gh_write(path, obj, msg, sha=None):
    body = {"message": msg, "content": base64.b64encode(json.dumps(obj, ensure_ascii=False, indent=2).encode()).decode(), "branch": GH_BRANCH}
    if sha:
        body["sha"] = sha
    req = urllib.request.Request(
        f"https://api.github.com/repos/{GH_REPO}/contents/{path}",
        data=json.dumps(body).encode(), method="PUT",
        headers={"Authorization": f"token {GH_TOKEN}", "Accept": "application/vnd.github.v3+json", "Content-Type": "application/json", "User-Agent": "ceraldi"}
    )
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read()).get("content", {}).get("sha")

def extract_p7m(data):
    for m in (b"<?xml", b"<FatturaElettronica", b"<p:FatturaElettronica"):
        i = data.find(m)
        if i != -1:
            chunk = data[i:]
            for end in (b"</FatturaElettronica>", b"</p:FatturaElettronica>"):
                j = chunk.rfind(end)
                if j != -1:
                    return chunk[:j+len(end)]
            return chunk
    i = 0
    while i < len(data)-10:
        if data[i] == 0x04:
            lb = data[i+1]
            if lb < 0x80:
                l, s = lb, i+2
            elif lb == 0x81:
                l, s = data[i+2], i+3
            elif lb == 0x82:
                l, s = (data[i+2]<<8)|data[i+3], i+4
            elif lb == 0x83:
                l, s = (data[i+2]<<16)|(data[i+3]<<8)|data[i+4], i+5
            else:
                i += 1
                continue
            if s+5 < len(data) and any(data[s:s+10].startswith(m) for m in (b"<?xml", b"<Fattura", b"<p:F")):
                return data[s:s+l]
        i += 1
    return None

def parse_xml(xml_bytes):
    try:
        root = ET.fromstring(xml_bytes)
    except Exception as e:
        log.warning(f"  ET error: {e} — raw: {xml_bytes[:300].decode('utf-8','ignore')}")
        return None

    ns = ""
    if root.tag.startswith("{"):
        ns = "{" + root.tag[1:root.tag.index("}")] + "}"

    if "FileMetadati" in root.tag or "FileMetadati" in root.tag.split("}")[-1]:
        log.info("  Skipping FileMetadati")
        return None

    def tx(path):
        for use_ns in (True, False):
            p = path.replace("/", f"/{ns}").replace("./", f"./{ns}") if use_ns and ns else path
            el = root.find(p)
            if el is not None and el.text:
                return el.text.strip()
        return ""

    fornitore = (
        tx("./FatturaElettronicaHeader/CedentePrestatore/DatiAnagrafici/Anagrafica/Denominazione") or
        (tx("./FatturaElettronicaHeader/CedentePrestatore/DatiAnagrafici/Anagrafica/Cognome") + " " +
         tx("./FatturaElettronicaHeader/CedentePrestatore/DatiAnagrafici/Anagrafica/Nome")).strip()
    )
    numero   = tx("./FatturaElettronicaBody/DatiGenerali/DatiGeneraliDocumento/Numero")
    data_raw = tx("./FatturaElettronicaBody/DatiGenerali/DatiGeneraliDocumento/Data")
    importo  = tx("./FatturaElettronicaBody/DatiGenerali/DatiGeneraliDocumento/ImportoTotaleDocumento")
    scadenza = tx("./FatturaElettronicaBody/DatiPagamento/DettaglioPagamento/DataScadenzaPagamento")
    iban     = tx("./FatturaElettronicaBody/DatiPagamento/DettaglioPagamento/IBAN")
    mod      = tx("./FatturaElettronicaBody/DatiPagamento/DettaglioPagamento/ModalitaPagamento")
    pag      = {"MP01": "contanti", "MP02": "assegno", "MP05": "bonifico"}.get(mod, "")

    # ── NUOVO: numero DDT di riferimento (DatiDDT) ──────────────────────────
    num_ddt  = tx("./FatturaElettronicaBody/DatiGenerali/DatiDDT/NumeroDDT")

    if not fornitore or not numero:
        log.warning(f"  XML incompleto — fornitore='{fornitore}' numero='{numero}'")
        log.warning(f"  Tag root: {root.tag}")
        log.warning(f"  XML raw: {xml_bytes[:500].decode('utf-8','ignore')}")
        return None

    try:
        imp = float(str(importo).replace(",", "."))
    except:
        imp = 0.0

    # ── NUOVO: salva il contenuto XML in base64 ─────────────────────────────
    # Limitato a 400 KB per non gonfiare l'indice (le fatture tipiche sono 5-50 KB)
    xml_b64 = ""
    if len(xml_bytes) <= 400_000:
        xml_b64 = base64.b64encode(xml_bytes).decode()
    else:
        log.warning(f"  XML troppo grande ({len(xml_bytes)} bytes), xmlBase64 non salvato")

    return {
        "id": f"pec_{int(time.time()*1000)}_{numero}",
        "tipo": "fattura",
        "fornitore": fornitore,
        "numero": numero,
        "data": data_raw[:10] if data_raw else "",
        "importo": imp,
        "scadenza": scadenza[:10] if scadenza else "",
        "pagamento": pag,
        "bonIban": iban,
        "numDdt": num_ddt,          # ── NUOVO: riferimento DDT dalla fattura
        "xmlBase64": xml_b64,       # ── NUOVO: contenuto XML completo in base64
        "stato": "da_pagare",
        "note": f"Importato da PEC ({datetime.now(timezone.utc).strftime('%d/%m/%Y')})",
        "rate": [],
        "source": "pec",
        "importedAt": datetime.now(timezone.utc).isoformat()
    }

def get_attachments(msg):
    attachments = []
    for part in msg.walk():
        fn = part.get_filename() or ""
        fn_l = fn.lower()
        xml_bytes = None

        if fn_l.endswith(".xml.p7m") or (fn_l.endswith(".p7m") and ".xml" in fn_l):
            raw = part.get_payload(decode=True)
            xml_bytes = extract_p7m(raw) if raw else None
        elif fn_l.endswith(".xml"):
            xml_bytes = part.get_payload(decode=True)
        elif fn_l.endswith(".zip"):
            raw = part.get_payload(decode=True)
            if raw:
                try:
                    with zipfile.ZipFile(io.BytesIO(raw)) as zf:
                        for name in zf.namelist():
                            if ".xml" in name.lower():
                                d = zf.read(name)
                                xml_bytes = extract_p7m(d) if name.lower().endswith(".p7m") else d
                                fn = name
                                break
                except:
                    pass

        if xml_bytes and len(xml_bytes) > 100:
            is_metadata = "_MT_" in fn.upper() or "METADAT" in fn.upper()
            is_p7m = fn_l.endswith(".p7m")
            priority = 0 if (is_p7m and not is_metadata) else (2 if is_metadata else 1)
            attachments.append((priority, fn, xml_bytes))

    attachments.sort(key=lambda x: x[0])
    return [(fn, xb) for _, fn, xb in attachments]

def sync():
    log.info("=== Avvio sync PEC -> GitHub ===")
    index, index_sha = gh_read("ceraldi_fatture_index.json", {"fatture": [], "lastSync": ""})
    processed, proc_sha = gh_read("processed_ids.json", [])
    log.info(f"Connessione IMAP {PEC_HOST}:{PEC_PORT}")
    new_count = 0

    with imaplib.IMAP4_SSL(PEC_HOST, PEC_PORT) as imap:
        imap.login(PEC_USER, PEC_PASS)

        _, folders = imap.list()
        log.info("Cartelle:")
        for f in (folders or []):
            log.info(f"  {f.decode() if isinstance(f, bytes) else f}")

        selected = False
        for try_name in ['"Fatturazione Elettronica"', 'Fatturazione Elettronica',
                         'INBOX.Fatturazione Elettronica', 'INBOX']:
            status, msgs = imap.select(try_name)
            if status == "OK":
                log.info(f"Cartella: {try_name} ({msgs[0].decode()} email)")
                selected = True
                break

        if not selected:
            log.error("Nessuna cartella selezionabile")
            return

        _, data = imap.search(None, "ALL")
        uids = data[0].split()
        log.info(f"Email: {len(uids)}")

        for uid in uids:
            uid_str = uid.decode()
            if uid_str in processed:
                continue

            try:
                _, msg_data = imap.fetch(uid, "(RFC822)")
                if not msg_data or not msg_data[0]:
                    processed.append(uid_str)
                    continue
                msg = email.message_from_bytes(msg_data[0][1])
            except Exception as e:
                log.warning(f"Fetch {uid_str}: {e}")
                processed.append(uid_str)
                continue

            attachments = get_attachments(msg)
            if not attachments:
                processed.append(uid_str)
                continue

            fattura_trovata = False
            for fn, xml_bytes in attachments:
                log.info(f"  Provo: {fn} ({len(xml_bytes)} bytes)")
                fattura = parse_xml(xml_bytes)
                if not fattura:
                    continue

                chiave = f"{fattura['fornitore']}|{fattura['numero']}"
                if any(f"{f.get('fornitore')}|{f.get('numero')}" == chiave for f in index.get("fatture", [])):
                    log.info(f"  Duplicato: {chiave}")
                    fattura_trovata = True
                    break

                index.setdefault("fatture", []).append(fattura)
                new_count += 1
                log.info(f"  + {fattura['fornitore']} n.{fattura['numero']} EUR {fattura['importo']}")
                fattura_trovata = True
                break

            processed.append(uid_str)

    index["lastSync"] = datetime.now(timezone.utc).isoformat()
    index["newCount"] = new_count
    gh_write("ceraldi_fatture_index.json", index, f"Sync: {new_count} nuove fatture", index_sha)
    gh_write("processed_ids.json", processed, "Aggiorna processed", proc_sha)
    log.info(f"=== Completata: {new_count} nuove fatture ===")

if __name__ == "__main__":
    sync()
    schedule.every(SYNC_INTERVAL_HOURS).hours.do(sync)
    log.info(f"Scheduler: ogni {SYNC_INTERVAL_HOURS}h")
    while True:
        schedule.run_pending()
        time.sleep(60)
