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
    Strategia 1: cerca il marker XML direttamente nel binario (p7m in chiaro o con header CMS minimo).
    Strategia 2: parsing DER/BER per trovare l'OCTET STRING che contiene l'XML.
    Strategia 3: scansione byte a byte per trovare sequenze XML valide.
    """
    # ── STRATEGIA 1: XML in chiaro nel buffer ──
    for marker in (b"<?xml", b"<FatturaElettronica", b"<p:FatturaElettronica", b"<ns2:FatturaElettronica"):
        i = data.find(marker)
        if i != -1:
            chunk = data[i:]
            for end in (b"</FatturaElettronica>", b"</p:FatturaElettronica>", b"</ns2:FatturaElettronica>"):
                j = chunk.rfind(end)
                if j != -1:
                    log.info("  p7m: XML trovato in chiaro")
                    return chunk[:j + len(end)]
            # Fine tag non trovato ma XML inizia — prendi tutto
            log.info("  p7m: XML parziale in chiaro")
            return chunk

    # ── STRATEGIA 2: parsing DER/BER ricorsivo ──
    def read_length(data, pos):
        """Legge la lunghezza BER a pos, restituisce (lunghezza, nuovo_pos)"""
        if pos >= len(data):
            return 0, pos
        lb = data[pos]
        if lb < 0x80:
            return lb, pos + 1
        elif lb == 0x81:
            if pos + 1 >= len(data): return 0, pos + 1
            return data[pos + 1], pos + 2
        elif lb == 0x82:
            if pos + 2 >= len(data): return 0, pos + 2
            return (data[pos + 1] << 8) | data[pos + 2], pos + 3
        elif lb == 0x83:
            if pos + 3 >= len(data): return 0, pos + 3
            return (data[pos + 1] << 16) | (data[pos + 2] << 8) | data[pos + 3], pos + 4
        elif lb == 0x84:
            if pos + 4 >= len(data): return 0, pos + 4
            return ((data[pos + 1] << 24) | (data[pos + 2] << 16) |
                    (data[pos + 3] << 8) | data[pos + 4]), pos + 5
        return 0, pos + 1

    def find_xml_in_der(data, depth=0):
        """Scansiona ricorsivamente struttura DER cercando OCTET STRING con XML"""
        if depth > 20 or len(data) < 4:
            return None
        pos = 0
        while pos < len(data) - 4:
            tag = data[pos]
            pos += 1
            length, pos = read_length(data, pos)
            if length <= 0 or pos + length > len(data):
                break
            content = data[pos:pos + length]

            # OCTET STRING (0x04) o SEQUENCE (0x30) o context-specific
            if tag in (0x04, 0x80, 0xA0):
                # Controlla se il contenuto inizia con XML
                for marker in (b"<?xml", b"<FatturaElettronica", b"<p:FatturaElettronica", b"<ns2:FatturaElettronica"):
                    if content.lstrip().startswith(marker) or content.find(marker) != -1:
                        i = content.find(marker)
                        if i == -1:
                            i = 0
                        chunk = content[i:]
                        for end in (b"</FatturaElettronica>", b"</p:FatturaElettronica>", b"</ns2:FatturaElettronica>"):
                            j = chunk.rfind(end)
                            if j != -1:
                                log.info(f"  p7m: XML trovato in OCTET STRING (tag=0x{tag:02x}, depth={depth})")
                                return chunk[:j + len(end)]
                        log.info(f"  p7m: XML parziale in OCTET STRING")
                        return chunk
                # Prova ricorsione sul contenuto
                if tag in (0x30, 0x31, 0xA0, 0xA1, 0xA2, 0xA3) or (tag & 0x20):
                    result = find_xml_in_der(content, depth + 1)
                    if result:
                        return result
                # Prova ricorsione anche su OCTET STRING (può contenere DER annidato)
                if tag == 0x04 and len(content) > 4 and content[0] in (0x30, 0x04):
                    result = find_xml_in_der(content, depth + 1)
                    if result:
                        return result
            elif tag in (0x30, 0x31) or (tag & 0x20):  # SEQUENCE/SET o constructed
                result = find_xml_in_der(content, depth + 1)
                if result:
                    return result

            pos += length

        return None

    result = find_xml_in_der(data)
    if result:
        return result

    # ── STRATEGIA 3: scansione bruta ──
    # Cerca qualsiasi sequenza che assomiglia a XML FatturaElettronica
    log.warning("  p7m: strategie DER fallite, provo scansione bruta")
    for marker in (b"<FatturaElettronica", b"<p:FatturaElettronica", b"<ns2:FatturaElettronica", b"<?xml"):
        i = 0
        while True:
            i = data.find(marker, i)
            if i == -1:
                break
            chunk = data[i:]
            for end in (b"</FatturaElettronica>", b"</p:FatturaElettronica>", b"</ns2:FatturaElettronica>"):
                j = chunk.rfind(end)
                if j != -1:
                    candidate = chunk[:j + len(end)]
                    # Verifica che sia XML valido
                    try:
                        ET.fromstring(candidate)
                        log.info(f"  p7m: XML trovato con scansione bruta a offset {i}")
                        return candidate
                    except:
                        pass
            i += 1

    log.warning("  p7m: impossibile estrarre XML")
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

    # ── FIX: il tipo è SEMPRE "fattura" ──
    # Le PEC ricevono solo fatture elettroniche, mai DDT.
    # DatiDDT dentro una fattura è un RIFERIMENTO al DDT di consegna,
    # non significa che il documento stesso sia un DDT.
    tipo = "fattura"

    log.info(f"  Parsed: {fornitore} n.{numero} {data_raw} EUR {imp} pag={pag} numDdt={num_ddt or '—'}")

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

    # ── FIX: correggi eventuali fatture nell'indice con tipo "ddt" errato ──
    # Una versione precedente dello script impostava tipo="ddt" quando numDdt era valorizzato.
    # Tutte le fatture importate da PEC sono sempre fatture elettroniche, mai DDT.
    corrette = 0
    for f in index.get("fatture", []):
        if f.get("tipo") == "ddt" and f.get("source") == "pec":
            f["tipo"] = "fattura"
            corrette += 1
    if corrette:
        log.info(f"  Corrette {corrette} fatture PEC con tipo 'ddt' → 'fattura'")

    log.info(f"Connessione IMAP {PEC_HOST}:{PEC_PORT}")
    new_count = 0

    with imaplib.IMAP4_SSL(PEC_HOST, PEC_PORT) as imap:
        imap.login(PEC_USER, PEC_PASS)

        _, folders = imap.list()
        log.info("Cartelle disponibili:")
        for f in (folders or []):
            log.info(f"  {f.decode() if isinstance(f, bytes) else f}")

        selected = False
        for try_name in ['"Fatturazione Elettronica"', 'Fatturazione Elettronica',
                         'INBOX.Fatturazione Elettronica', 'INBOX']:
            status, msgs = imap.select(try_name)
            if status == "OK":
                log.info(f"Cartella selezionata: {try_name} ({msgs[0].decode()} email)")
                selected = True

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

                    for fn, xml_bytes in attachments:
                        log.info(f"  Provo: {fn} ({len(xml_bytes)} bytes)")
                        fattura = parse_xml(xml_bytes)
                        if not fattura:
                            log.warning(f"  parse_xml fallito per {fn}")
                            continue

                        # ── FIX: chiave include anche la data per evitare falsi duplicati ──
                        # Due fatture dello stesso fornitore con stesso numero ma date diverse
                        # (es. numerazione annuale che riparte) non sono duplicati.
                        chiave = f"{fattura['fornitore']}|{fattura['numero']}|{fattura['data']}"
                        esistente = next(
                            (f for f in index.get("fatture", [])
                             if f"{f.get('fornitore')}|{f.get('numero')}|{f.get('data','')}" == chiave),
                            None
                        )

                        if esistente:
                            # Fattura già nell'indice — recupera XML se manca
                            recovery_done = index.get("xml_recovery_done", False)
                            if not recovery_done and not esistente.get("xmlGithubPath"):
                                safe_forn = "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in fattura["fornitore"])[:40]
                                safe_num  = "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in fattura["numero"])[:30]
                                xml_path  = f"fatture_xml/{safe_forn}_{safe_num}.xml"
                                try:
                                    existing_sha = gh_get_sha(xml_path)
                                    gh_write_raw(xml_path, xml_bytes, f"XML recuperato: {fattura['fornitore']} n.{fattura['numero']}", existing_sha)
                                    esistente["xmlGithubPath"] = xml_path
                                    new_count += 1
                                    log.info(f"  XML recuperato: {xml_path}")
                                except Exception as xe:
                                    log.warning(f"  Recupero XML fallito: {xe}")
                            else:
                                log.info(f"  Duplicato (già in indice): {chiave}")
                            break

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
                        log.info(f"  + {fattura['fornitore']} n.{fattura['numero']} EUR {fattura['importo']}")
                        break

                    processed.append(uid_str)

    index["lastSync"] = datetime.now(timezone.utc).isoformat()
    index["newCount"] = new_count
    gh_write("ceraldi_fatture_index.json", index, f"Sync: {new_count} nuove fatture, {corrette} tipo corretti", index_sha)
    gh_write("processed_ids.json", processed, "Aggiorna processed", proc_sha)
    log.info(f"=== Completata: {new_count} nuove fatture, {corrette} tipo corretti ===")

if __name__ == "__main__":
    sync()
    log.info("=== GitHub Actions: sync completata ===")
