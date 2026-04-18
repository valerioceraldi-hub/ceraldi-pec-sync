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
    content = base64.b64encode(json.dumps(obj, ensure_ascii=False, indent=2).encode()).decode()
    def _do_write(sha_to_use):
        body = {"message": msg, "content": content, "branch": GH_BRANCH}
        if sha_to_use:
            body["sha"] = sha_to_use
        req = urllib.request.Request(
            f"https://api.github.com/repos/{GH_REPO}/contents/{path}",
            data=json.dumps(body).encode(), method="PUT",
            headers={"Authorization": f"token {GH_TOKEN}", "Accept": "application/vnd.github.v3+json", "Content-Type": "application/json", "User-Agent": "ceraldi"}
        )
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read()).get("content", {}).get("sha")
    try:
        return _do_write(sha)
    except Exception as e:
        if "422" in str(e) or "409" in str(e):
            log.warning(f"  gh_write {str(e)[:40]} su {path}, rileggo sha e riprovo")
            fresh_sha = gh_get_sha(path)
            return _do_write(fresh_sha)
        raise

def gh_write_raw(path, content_bytes, msg, sha=None, max_retries=3):
    """
    FIX v10: aggiunto retry automatico con backoff.
    Le 409 Conflict (sha stale) e 422 Unprocessable sono recuperabili
    rileggendo lo SHA aggiornato e riprovando.
    """
    last_exc = None
    for attempt in range(max_retries):
        try:
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
        except Exception as e:
            last_exc = e
            err_str = str(e)
            if "409" in err_str or "422" in err_str:
                # Conflict: rileggi lo SHA e riprova
                log.warning(f"  gh_write_raw {err_str[:40]} su {path}, attempt {attempt+1}/{max_retries}")
                time.sleep(1.5 * (attempt + 1))
                sha = gh_get_sha(path)
                continue
            elif "403" in err_str or "rate limit" in err_str.lower():
                # Rate limit: attesa lunga
                log.warning(f"  gh_write_raw rate limit su {path}, attendo 60s")
                time.sleep(60)
                continue
            else:
                # Errore non recuperabile
                raise
    # Tutti i tentativi falliti
    raise last_exc if last_exc else Exception(f"Upload fallito dopo {max_retries} tentativi")

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
    [Funzione immutata dalla v9 — funziona correttamente]
    """
    def read_len(data, pos):
        if pos >= len(data): return 0, pos
        lb = data[pos]; pos += 1
        if lb < 0x80: return lb, pos
        elif lb == 0x81: return data[pos], pos + 1
        elif lb == 0x82: return (data[pos] << 8) | data[pos+1], pos + 2
        elif lb == 0x83: return (data[pos] << 16) | (data[pos+1] << 8) | data[pos+2], pos + 3
        elif lb == 0x84: return ((data[pos] << 24) | (data[pos+1] << 16) | (data[pos+2] << 8) | data[pos+3]), pos + 4
        elif lb == 0x80: return -1, pos
        return 0, pos

    XML_MARKERS = (b"<?xml", b"<p:FatturaElettronica", b"<FatturaElettronica", b"<ns2:FatturaElettronica")
    XML_ENDS    = (b"</p:FatturaElettronica>", b"</FatturaElettronica>", b"</ns2:FatturaElettronica>")

    def is_xml_content(chunk):
        if not chunk: return False
        printable = sum(1 for b in chunk if 0x09 <= b <= 0x7e or b in (0x0a, 0x0d))
        return printable / len(chunk) > 0.85

    def find_xml_end(data):
        for end in XML_ENDS:
            j = data.rfind(end)
            if j != -1:
                return j + len(end)
        return -1

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
                    pass

    xml_chunks = []
    collecting = False
    pos = 0

    while pos < len(data) - 4:
        tag = data[pos]
        pos += 1
        length, pos = read_len(data, pos)

        is_constructed = bool(tag & 0x20)
        is_octet_type  = (tag & 0x1f) == 0x04

        if length == -1:
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
            pass

        else:
            pos += length

    log.warning("  p7m: strategie 1-2 fallite, provo scansione bruta (strategia 3)")
    for marker in XML_MARKERS:
        i = data.find(marker)
        if i == -1: continue
        chunk = data[i:]
        end_pos = find_xml_end(chunk)
        if end_pos == -1: continue
        raw = chunk[:end_pos]
        cleaned = bytearray()
        j = 0
        while j < len(raw):
            b = raw[j]
            if b >= 0x09:
                cleaned.append(b)
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

    tipo = "fattura"

    log.info(f"  Parsed OK: {fornitore} n.{numero} {data_raw} EUR {imp} pag={pag} numDdt={num_ddt or '—'}")

    # FIX v10: id DETERMINISTICO (non dipende da time.time())
    # Se lo stesso XML viene riprocessato, ottieni sempre lo stesso id.
    # Questo previene duplicati se processed_ids.json viene resettato.
    safe_forn_id = "".join(c if c.isalnum() else "_" for c in fornitore)[:30]
    safe_num_id  = "".join(c if c.isalnum() else "_" for c in numero)[:30]
    data_id = (data_raw[:10] if data_raw else "nodate").replace("-","")
    deterministic_id = f"pec_{safe_forn_id}_{safe_num_id}_{data_id}"

    return {
        "id": deterministic_id,
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


def upload_xml_to_github(fattura, xml_bytes):
    """
    FIX v10: tenta SEMPRE l'upload con retry, e ritorna il path solo se effettivamente salvato.
    Se fallisce dopo i retry, ritorna None e il chiamante decide cosa fare.
    """
    safe_forn = "".join(c if c.isalnum() or c in "-_." else "_" for c in fattura["fornitore"])[:40]
    safe_num  = "".join(c if c.isalnum() or c in "-_." else "_" for c in fattura["numero"])[:30]
    xml_path  = f"fatture_xml/{safe_forn}_{safe_num}.xml"

    try:
        existing_sha = gh_get_sha(xml_path)
        gh_write_raw(xml_path, xml_bytes,
                     f"Fattura {fattura['fornitore']} n.{fattura['numero']}",
                     existing_sha)
        log.info(f"  ✓ XML salvato: {xml_path}")
        return xml_path
    except Exception as xe:
        log.error(f"  ✗ Upload XML FALLITO per {fattura['fornitore']} n.{fattura['numero']}: {xe}")
        return None


def recupera_xml_mancanti(index, index_sha):
    """
    FIX v10: funzione di recupero per fatture già in indice SENZA xmlGithubPath.
    Cicla sulle fatture dell'indice, se un XML esiste già nella cartella fatture_xml
    col nome atteso, popola il campo nel record. Non riscarica le PEC.
    """
    recuperate = 0
    for f in index.get("fatture", []):
        # Se il campo manca o è vuoto, prova a recuperarlo
        if f.get("xmlGithubPath"):
            continue
        if not f.get("fornitore") or not f.get("numero"):
            continue
        safe_forn = "".join(c if c.isalnum() or c in "-_." else "_" for c in f["fornitore"])[:40]
        safe_num  = "".join(c if c.isalnum() or c in "-_." else "_" for c in f["numero"])[:30]
        expected_path = f"fatture_xml/{safe_forn}_{safe_num}.xml"
        # Controlla se il file esiste già nel repo
        sha = gh_get_sha(expected_path)
        if sha:
            f["xmlGithubPath"] = expected_path
            recuperate += 1
            log.info(f"  ↻ Recuperato xmlGithubPath per {f['fornitore']} n.{f['numero']} → {expected_path}")
    if recuperate:
        log.info(f"✓ {recuperate} fatture hanno ritrovato l'XML già esistente nel repo")
    return recuperate


def sync():
    log.info("=== Avvio sync PEC -> GitHub (v10) ===")
    index, index_sha = gh_read("ceraldi_fatture_index.json", {"fatture": [], "lastSync": ""})
    processed, proc_sha = gh_read("processed_ids.json", [])

    # ── Fix: tipo "ddt" errato ──
    corrette_tipo = 0
    for f in index.get("fatture", []):
        if f.get("tipo") == "ddt" and f.get("source") == "pec":
            f["tipo"] = "fattura"
            corrette_tipo += 1
    if corrette_tipo:
        log.info(f"  Corrette {corrette_tipo} fatture PEC con tipo 'ddt' → 'fattura'")

    # ── FIX v10: recupera xmlGithubPath per fatture vecchie che ce l'hanno vuoto ──
    # Per le fatture che nel repo hanno già l'XML ma nell'indice non c'è il link.
    recuperate = recupera_xml_mancanti(index, index_sha)

    log.info(f"Connessione IMAP {PEC_HOST}:{PEC_PORT}")
    new_count = 0
    xml_upload_failures = 0

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

                    # Dedup
                    chiave = f"{fattura['fornitore']}|{fattura['numero']}|{fattura['data']}"
                    gia_esistente = None
                    for f_idx in index.get("fatture", []):
                        if f"{f_idx.get('fornitore')}|{f_idx.get('numero')}|{f_idx.get('data','')}" == chiave:
                            gia_esistente = f_idx
                            break

                    if gia_esistente:
                        # Caso speciale: la fattura è già in indice ma SENZA xmlGithubPath.
                        # Questa è la situazione delle tue prime 10 fatture (Timas, Amodio, ecc).
                        # Se ora abbiamo l'XML, dobbiamo popolare il campo invece di saltarla.
                        if not gia_esistente.get("xmlGithubPath"):
                            log.info(f"  Fattura già in indice SENZA xml, tento upload: {chiave}")
                            xml_path = upload_xml_to_github(fattura, xml_bytes)
                            if xml_path:
                                gia_esistente["xmlGithubPath"] = xml_path
                                log.info(f"  ✓ xmlGithubPath popolato retroattivamente")
                            else:
                                xml_upload_failures += 1
                        else:
                            log.info(f"  Già in indice CON xml: {chiave}")
                        imported = True
                        break

                    # ── FIX v10: carica XML PRIMA di aggiungere all'indice ──
                    # Se l'upload fallisce, NON aggiungiamo la fattura all'indice
                    # (così la prossima run riprova). E NON marchiamo la email come processata.
                    xml_path = upload_xml_to_github(fattura, xml_bytes)
                    if not xml_path:
                        log.error(f"  ✗ Skip fattura {fattura['fornitore']} n.{fattura['numero']}: upload XML fallito")
                        xml_upload_failures += 1
                        imported = False
                        break  # non processare altri allegati della stessa email, la riprocesseremo

                    # Upload riuscito → aggiungi all'indice
                    fattura["xmlGithubPath"] = xml_path
                    index.setdefault("fatture", []).append(fattura)
                    new_count += 1
                    imported = True
                    log.info(f"  + {fattura['fornitore']} n.{fattura['numero']} EUR {fattura['importo']} (xml: {xml_path})")
                    break

                if imported:
                    # Email processata con successo → non rielaborare
                    processed.append(uid_str)
                else:
                    log.warning(f"  {uid_str}: non importata (verrà ritentata alla prossima run)")
                    # NON aggiungere a processed — così la prossima run ritenta

    index["lastSync"] = datetime.now(timezone.utc).isoformat()
    index["newCount"] = new_count

    # Salva sempre l'indice (anche se le modifiche sono solo retroattive)
    gh_write("ceraldi_fatture_index.json", index,
             f"Sync v10: {new_count} nuove, {corrette_tipo} tipo corretti, {recuperate} xml recuperati", index_sha)
    gh_write("processed_ids.json", processed, "Aggiorna processed", proc_sha)

    log.info(f"=== Completata: {new_count} nuove, {corrette_tipo} tipo corretti, {recuperate} xml retroattivi ===")
    if xml_upload_failures:
        log.warning(f"⚠️ {xml_upload_failures} upload XML falliti — verranno ritentati alla prossima run")

if __name__ == "__main__":
    sync()
    log.info("=== GitHub Actions: sync completata ===")
