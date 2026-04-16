import imaplib, email, os, json, time, logging, zipfile, io, urllib.request, base64
from datetime import datetime, timezone
from xml.etree import ElementTree as ET

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("ceraldi-pec")

PEC_HOST  = os.environ.get("PEC_HOST", "imaps.pec.aruba.it")
PEC_PORT  = int(os.environ.get("PEC_PORT", "993"))
PEC_USER  = os.environ["PEC_USER"]
PEC_PASS  = os.environ["PEC_PASS"]
GH_TOKEN  = os.environ["GH_TOKEN"]
GH_REPO   = os.environ["GH_REPO"]
GH_BRANCH = os.environ.get("GH_BRANCH", "main")

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

def gh_write_raw(path, content_bytes, msg, sha=None):
    body = {
        "message": msg,
        "content": base64.b64encode(content_bytes).decode(),
        "branch": GH_BRANCH
    }
    if sha:
        body["sha"] = sha
    req = urllib.request.Request(
        f"https://api.github.com/repos/{GH_REPO}/contents/{path}",
        data=json.dumps(body).encode(), method="PUT",
        headers={"Authorization": f"token {GH_TOKEN}", "Accept": "application/vnd.github.v3+json",
                 "Content-Type": "application/json", "User-Agent": "ceraldi"}
    )
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read()).get("content", {}).get("sha")

def gh_get_sha(path):
    req = urllib.request.Request(
        f"https://api.github.com/repos/{GH_REPO}/contents/{path}?ref={GH_BRANCH}",
        headers={"Authorization": f"token {GH_TOKEN}", "Accept": "application/vnd.github.v3+json", "User-Agent": "ceraldi"}
    )
    try:
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read()).get("sha")
    except:
        return None

def extract_p7m(data):
    """
    Estrae XML da un file .p7m (PKCS#7/CMS).

    Gestisce tre casi reali riscontrati:
    1. XML in chiaro nel buffer (p7m con header CMS minimo)
    2. XML in un singolo OCTET STRING BER/DER
    3. XML spezzato in più OCTET STRING consecutivi da 1000 bytes (es. KIMBO via Intesa)
       In questo caso i byte dell'header BER finiscono nel mezzo dell'XML e lo corrompono —
       la soluzione è concatenare i chunk e ricostruire l'XML pulito.
    """

    def read_len(data, pos):
        if pos >= len(data): return 0, pos
        lb = data[pos]; pos += 1
        if lb < 0x80: return lb, pos
        elif lb == 0x81: return data[pos], pos + 1
        elif lb == 0x82: return (data[pos] << 8) | data[pos+1], pos + 2
        elif lb == 0x83: return (data[pos] << 16) | (data[pos+1] << 8) | data[pos+2], pos + 3
        elif lb == 0x84: return ((data[pos] << 24) | (data[pos+1] << 16) | (data[pos+2] << 8) | data[pos+3]), pos + 4
        elif lb == 0x80: return -1, pos  # lunghezza indefinita BER
        return 0, pos

    XML_MARKERS = (b"<?xml", b"<p:FatturaElettronica", b"<FatturaElettronica", b"<ns2:FatturaElettronica")
    XML_ENDS    = (b"</p:FatturaElettronica>", b"</FatturaElettronica>", b"</ns2:FatturaElettronica>")

    def is_xml_content(chunk):
        """Controlla se un chunk contiene testo XML (>85% caratteri stampabili)"""
        if not chunk: return False
        printable = sum(1 for b in chunk if 0x09 <= b <= 0x7e or b in (0x0a, 0x0d))
        return printable / len(chunk) > 0.85

    def find_xml_end(data):
        for end in XML_ENDS:
            j = data.rfind(end)
            if j != -1:
                return j + len(end)
        return -1

    # ── STRATEGIA 1: XML in chiaro, non spezzato ──
    for marker in XML_MARKERS:
        i = data.find(marker)
        if i != -1:
            chunk = data[i:]
            end_pos = find_xml_end(chunk)
            if end_pos != -1:
                candidate = chunk[:end_pos]
                try:
                    ET.fromstring(candidate)
                    log.info("  p7m: XML in chiaro (strategia 1)")
                    return candidate
                except:
                    pass  # Malformato — probabilmente spezzato, continua con strategia 2

    # ── STRATEGIA 2: concatenazione OCTET STRING consecutivi ──
    # Usata quando l'XML è spezzato in chunk da N bytes (es. 1000) dentro struttura BER
    # con lunghezze indefinite (0x80). I byte dell'header BER tra un chunk e l'altro
    # corrompono l'XML se si prende il buffer grezzo — occorre estrarre solo i payload.
    xml_chunks = []
    collecting = False
    pos = 0

    while pos < len(data) - 4:
        tag = data[pos]
        pos += 1
        length, pos = read_len(data, pos)

        is_constructed = bool(tag & 0x20)        # bit 5 = constructed
        is_octet_type  = (tag & 0x1f) == 0x04    # OCTET STRING (primitive 0x04 o constructed 0x24)

        if length == -1:
            # Lunghezza indefinita — continua scansione lineare dentro
            if is_octet_type and not is_constructed:
                end_indef = data.find(b'\x00\x00', pos)
                if end_indef == -1: break
                content = data[pos:end_indef]
                pos = end_indef + 2
                if not collecting:
                    for marker in XML_MARKERS:
                        mi = content.find(marker)
                        if mi != -1:
                            collecting = True
                            xml_chunks = [content[mi:]]
                            break
                else:
                    if is_xml_content(content):
                        xml_chunks.append(content)
            continue

        if length <= 0: continue
        if pos + length > len(data): break

        content = data[pos:pos + length]

        if is_octet_type and not is_constructed:
            # OCTET STRING primitivo: contiene dati XML
            if not collecting:
                for marker in XML_MARKERS:
                    mi = content.find(marker)
                    if mi != -1:
                        collecting = True
                        xml_chunks = [content[mi:]]
                        break
            else:
                if is_xml_content(content):
                    xml_chunks.append(content)
                else:
                    collecting = False
                    xml_chunks = []

            if collecting and xml_chunks:
                combined = b''.join(xml_chunks)
                end_pos = find_xml_end(combined)
                if end_pos != -1:
                    candidate = combined[:end_pos]
                    try:
                        ET.fromstring(candidate)
                        log.info(f"  p7m: XML da {len(xml_chunks)} chunk OCTET STRING (strategia 2)")
                        return candidate
                    except:
                        pass

            pos += length

        elif is_constructed or tag in (0x30, 0x31, 0xa0, 0xa1, 0xa2, 0xa3):
            # Container constructed: non skippiamo, entriamo linearmente nel contenuto
            pass  # pos NON avanzato di length

        else:
            pos += length

    # ── STRATEGIA 3: scansione bruta con pulizia byte spuri ──
    # Ultimo tentativo: estrae tutto il testo tra il primo marker XML e l'ultimo tag di chiusura,
    # filtrando i byte non-XML (header BER) che potrebbero essere nel mezzo.
    log.warning("  p7m: strategie 1-2 fallite, provo scansione bruta (strategia 3)")
    for marker in XML_MARKERS:
        i = data.find(marker)
        if i == -1: continue
        chunk = data[i:]
        end_pos = find_xml_end(chunk)
        if end_pos == -1: continue
        raw = chunk[:end_pos]
        # Filtra byte non-XML: rimuovi sequenze di byte con valore < 0x09 o > 0x7e
        # che non siano parte di tag XML legittimi
        cleaned = bytearray()
        j = 0
        while j < len(raw):
            b = raw[j]
            if b >= 0x09:  # carattere stampabile o whitespace XML
                cleaned.append(b)
            else:
                # Salta sequenza di byte non-XML (header BER intrusivo)
                pass
            j += 1
        try:
            ET.fromstring(bytes(cleaned))
            log.info("  p7m: XML estratto con scansione bruta + pulizia")
            return bytes(cleaned)
        except:
            pass

    log.warning("  p7m: impossibile estrarre XML con nessuna strategia")
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
    num_ddt  = tx("./FatturaElettronicaBody/DatiGenerali/DatiDDT/NumeroDDT")

    if not fornitore or not numero:
        log.warning(f"  XML incompleto — fornitore='{fornitore}' numero='{numero}'")
        return None

    try:
        imp = float(str(importo).replace(",", "."))
    except:
        imp = 0.0

    # Il tipo è SEMPRE "fattura": dalla PEC arrivano solo fatture elettroniche.
    # DatiDDT dentro una fattura è un riferimento al DDT di consegna,
    # NON significa che il documento stesso sia un DDT.
    tipo = "fattura"

    log.info(f"  Parsed OK: {fornitore} n.{numero} {data_raw} EUR {imp} pag={pag} numDdt={num_ddt or '—'}")

    return {
        "id": f"pec_{int(time.time()*1000)}_{numero}",
        "tipo": tipo,
        "fornitore": fornitore,
        "numero": numero,
        "data": data_raw[:10] if data_raw else "",
        "importo": imp,
        "scadenza": scadenza[:10] if scadenza else "",
        "pagamento": pag,
        "bonIban": iban,
        "numDdt": num_ddt,
        "xmlGithubPath": "",
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

    # ── Correggi fatture PEC con tipo "ddt" errato nell'indice ──
    # Una versione precedente dello script impostava tipo="ddt" quando numDdt era valorizzato.
    # Tutte le fatture importate da PEC sono sempre fatture elettroniche.
    corrette_tipo = 0
    for f in index.get("fatture", []):
        if f.get("tipo") == "ddt" and f.get("source") == "pec":
            f["tipo"] = "fattura"
            corrette_tipo += 1
    if corrette_tipo:
        log.info(f"  Corrette {corrette_tipo} fatture PEC con tipo 'ddt' → 'fattura'")

    log.info(f"Connessione IMAP {PEC_HOST}:{PEC_PORT}")
    new_count = 0

    with imaplib.IMAP4_SSL(PEC_HOST, PEC_PORT) as imap:
        imap.login(PEC_USER, PEC_PASS)

        _, folders = imap.list()
        log.info("Cartelle disponibili:")
        for f in (folders or []):
            log.info(f"  {f.decode() if isinstance(f, bytes) else f}")

        for try_name in ['"Fatturazione Elettronica"', 'Fatturazione Elettronica',
                         'INBOX.Fatturazione Elettronica', 'INBOX']:
            status, msgs = imap.select(try_name)
            if status != "OK":
                continue

            log.info(f"Cartella selezionata: {try_name} ({msgs[0].decode()} email)")
            _, data = imap.search(None, "ALL")
            uids = data[0].split()
            log.info(f"Email trovate: {len(uids)}")

            for uid in uids:
                uid_str = f"{try_name}:{uid.decode()}"
                uid_bare = uid.decode()
                if uid_str in processed or uid_bare in processed:
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
                    log.info(f"  {uid_str}: nessun allegato XML trovato")
                    processed.append(uid_str)
                    continue

                imported = False
                for fn, xml_bytes in attachments:
                    log.info(f"  Provo: {fn} ({len(xml_bytes)} bytes)")
                    fattura = parse_xml(xml_bytes)
                    if not fattura:
                        log.warning(f"  parse_xml fallito per {fn}")
                        continue

                    # ── Deduplicazione sull'indice ──
                    # La chiave include fornitore + numero + data per evitare falsi positivi
                    # su fatture con stessa numerazione di anni diversi.
                    # IMPORTANTE: l'indice non è la fonte di verità finale — lo è Supabase.
                    # Se una fattura è già nell'indice ma con dati errati (es. tipo=ddt),
                    # è già stata corretta sopra. Se è nell'indice con dati corretti,
                    # è un vero duplicato e la saltiamo.
                    chiave = f"{fattura['fornitore']}|{fattura['numero']}|{fattura['data']}"
                    gia_in_indice = any(
                        f"{f.get('fornitore')}|{f.get('numero')}|{f.get('data','')}" == chiave
                        for f in index.get("fatture", [])
                    )

                    if gia_in_indice:
                        log.info(f"  Già in indice: {chiave}")
                        imported = True  # considera gestita
                        break

                    # Salva XML su GitHub
                    safe_forn = "".join(c if c.isalnum() or c in "-_." else "_" for c in fattura["fornitore"])[:40]
                    safe_num  = "".join(c if c.isalnum() or c in "-_." else "_" for c in fattura["numero"])[:30]
                    xml_path  = f"fatture_xml/{safe_forn}_{safe_num}.xml"
                    try:
                        existing_sha = gh_get_sha(xml_path)
                        gh_write_raw(xml_path, xml_bytes, f"Fattura {fattura['fornitore']} n.{fattura['numero']}", existing_sha)
                        fattura["xmlGithubPath"] = xml_path
                        log.info(f"  XML salvato: {xml_path}")
                    except Exception as xe:
                        log.warning(f"  Salvataggio XML fallito: {xe}")

                    index.setdefault("fatture", []).append(fattura)
                    new_count += 1
                    imported = True
                    log.info(f"  + {fattura['fornitore']} n.{fattura['numero']} EUR {fattura['importo']}")
                    break

                if not imported:
                    log.warning(f"  {uid_str}: nessuna fattura importata (tutti i parse_xml falliti)")

                processed.append(uid_str)

    index["lastSync"] = datetime.now(timezone.utc).isoformat()
    index["newCount"] = new_count
    gh_write("ceraldi_fatture_index.json", index,
             f"Sync: {new_count} nuove, {corrette_tipo} tipo corretti", index_sha)
    gh_write("processed_ids.json", processed, "Aggiorna processed", proc_sha)
    log.info(f"=== Completata: {new_count} nuove fatture, {corrette_tipo} tipo corretti ===")

if __name__ == "__main__":
    sync()
    log.info("=== GitHub Actions: sync completata ===")
