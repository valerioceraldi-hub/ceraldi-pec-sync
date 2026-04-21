import imaplib, email, os, json, time, logging, zipfile, io, urllib.request, base64, re
from email.header import decode_header
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

def _clean_xml_candidate(candidate):
    if not candidate:
        return None
    candidate = bytes(candidate).replace(b"\x00", b"").strip()
    first_lt = candidate.find(b"<")
    if first_lt > 0:
        candidate = candidate[first_lt:]
    if not candidate:
        return None

    attempts = [candidate]
    xml_decl_match = re.search(br'<\?xml[^>]*\?>', candidate, flags=re.IGNORECASE)
    if xml_decl_match:
        attempts.append(candidate[xml_decl_match.start():])
    text = candidate.decode('utf-8', 'ignore')
    text = text[text.find('<'):] if '<' in text else text
    attempts.append(text.encode('utf-8', 'ignore'))
    attempts.append(candidate.replace(b"\r\n", b"\n"))

    seen = set()
    for attempt in attempts:
        if not attempt or attempt in seen:
            continue
        seen.add(attempt)
        try:
            ET.fromstring(attempt)
            return attempt
        except Exception:
            continue
    return None


def extract_p7m(data):
    """
    Estrae XML da un file .p7m (PKCS#7/CMS) anche quando Aruba allega
    buste con filename solo .p7m o con byte sporchi prima/dopo l'XML.
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

    XML_MARKERS = (
        b"<?xml",
        b"<FatturaElettronica",
        b"<p:FatturaElettronica",
        b"<ns2:FatturaElettronica",
        b"<ns3:FatturaElettronica",
    )
    XML_END_RE = re.compile(br'</(?:[A-Za-z0-9_]+:)?FatturaElettronica\s*>', re.IGNORECASE)
    XML_FULL_RE = re.compile(
        br'(?:<\?xml[^>]*\?>\s*)?<(?P<prefix>[A-Za-z0-9_]+:)?FatturaElettronica\b[\s\S]*?</(?P=prefix)?FatturaElettronica\s*>',
        re.IGNORECASE,
    )

    def find_xml_end(blob):
        last = None
        for match in XML_END_RE.finditer(blob):
            last = match.end()
        return last if last is not None else -1

    for marker in XML_MARKERS:
        i = data.find(marker)
        if i != -1:
            chunk = data[i:]
            end_pos = find_xml_end(chunk)
            if end_pos != -1:
                candidate = _clean_xml_candidate(chunk[:end_pos])
                if candidate:
                    log.info("  p7m: XML in chiaro (strategia 1)")
                    return candidate

    xml_chunks = []
    collecting = False
    pos = 0
    while pos < len(data) - 4:
        tag = data[pos]
        pos += 1
        length, pos = read_len(data, pos)

        is_constructed = bool(tag & 0x20)
        is_octet_type = (tag & 0x1f) == 0x04

        if length == -1:
            if is_octet_type and not is_constructed:
                end_indef = data.find(b'\x00\x00', pos)
                if end_indef == -1:
                    break
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
                    xml_chunks.append(content)
            continue

        if length <= 0:
            continue
        if pos + length > len(data):
            break

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
                xml_chunks.append(content)

            if collecting and xml_chunks:
                combined = b''.join(xml_chunks)
                end_pos = find_xml_end(combined)
                if end_pos != -1:
                    candidate = _clean_xml_candidate(combined[:end_pos])
                    if candidate:
                        log.info(f"  p7m: XML da {len(xml_chunks)} chunk OCTET STRING (strategia 2)")
                        return candidate
            pos += length
        elif is_constructed or tag in (0x30, 0x31, 0xa0, 0xa1, 0xa2, 0xa3):
            pass
        else:
            pos += length

    log.warning("  p7m: strategie 1-2 fallite, provo regex binaria (strategia 3)")
    match = XML_FULL_RE.search(data)
    if match:
        candidate = _clean_xml_candidate(match.group(0))
        if candidate:
            log.info("  p7m: XML estratto con regex binaria")
            return candidate

    log.warning("  p7m: strategia 3 fallita, provo scansione testuale (strategia 4)")
    text = data.decode('latin-1', 'ignore')
    text_match = re.search(r'(?:<\?xml[^>]*\?>\s*)?<(?P<prefix>[A-Za-z0-9_]+:)?FatturaElettronica\b[\s\S]*?</(?P=prefix)?FatturaElettronica\s*>', text, re.IGNORECASE)
    if text_match:
        candidate = _clean_xml_candidate(text_match.group(0).encode('utf-8', 'ignore'))
        if candidate:
            log.info("  p7m: XML estratto con scansione testuale")
            return candidate

    log.warning("  p7m: impossibile estrarre XML con nessuna strategia")
    return None

def parse_xml(xml_bytes):
    if not xml_bytes:
        return None

    xml_bytes = bytes(xml_bytes).replace(b"\x00", b"").strip()
    first_lt = xml_bytes.find(b"<")
    if first_lt > 0:
        xml_bytes = xml_bytes[first_lt:]

    try:
        root = ET.fromstring(xml_bytes)
    except Exception as e:
        log.warning(f"  ET error: {e} — raw: {xml_bytes[:300].decode('utf-8','ignore')}")
        return None

    local_tag = root.tag.split("}")[-1].lower()
    if "fatturaelettronica" not in local_tag:
        if any(token in local_tag for token in ("filemetadati", "postacert", "ricevutaconsegna", "daticert", "segnatura", "esito")):
            log.info(f"  Skipping XML di sistema: {root.tag}")
        else:
            log.info(f"  Skipping XML non fattura: {root.tag}")
        return None

    ns = ""
    if root.tag.startswith("{"):
        ns = "{" + root.tag[1:root.tag.index("}")] + "}"

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
    except Exception:
        imp = 0.0

    tipo = "fattura"

    log.info(f"  Parsed OK: {fornitore} n.{numero} {data_raw} EUR {imp} pag={pag} numDdt={num_ddt or '—'}")

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

def decode_filename(raw_name):
    if not raw_name:
        return ""
    try:
        parts = []
        for part, encoding in decode_header(raw_name):
            if isinstance(part, bytes):
                parts.append(part.decode(encoding or "utf-8", "ignore"))
            else:
                parts.append(part)
        return "".join(parts)
    except Exception:
        return raw_name


def classify_attachment(filename):
    name = (filename or "").lower()
    if any(token in name for token in ("daticert", "postacert", "smime", "ricevuta", "esitoatto", "segnatura")):
        return "system"
    if "_mt_" in name or "metadat" in name or "filemetadati" in name:
        return "metadata"
    if name.endswith(".p7m") or any(token in name for token in ("_at_", "fattura", "invoice")):
        return "invoice"
    if name.endswith(".xml"):
        return "xml"
    return "other"


def add_attachment_candidate(attachments, filename, xml_bytes):
    if not xml_bytes or len(xml_bytes) <= 100:
        return
    kind = classify_attachment(filename)
    priority_map = {"invoice": 0, "xml": 1, "metadata": 2, "system": 3, "other": 4}
    attachments.append((priority_map.get(kind, 4), filename, xml_bytes, kind))


def get_attachments(msg):
    attachments = []
    for part in msg.walk():
        fn = decode_filename(part.get_filename() or "")
        if not fn:
            continue
        fn_l = fn.lower()

        if fn_l.endswith(".p7m"):
            raw = part.get_payload(decode=True)
            xml_bytes = extract_p7m(raw) if raw else None
            add_attachment_candidate(attachments, fn, xml_bytes)
            continue

        if fn_l.endswith(".xml"):
            xml_bytes = part.get_payload(decode=True)
            add_attachment_candidate(attachments, fn, xml_bytes)
            continue

        if fn_l.endswith(".zip"):
            raw = part.get_payload(decode=True)
            if not raw:
                continue
            try:
                with zipfile.ZipFile(io.BytesIO(raw)) as zf:
                    for name in zf.namelist():
                        name_l = name.lower()
                        if not (name_l.endswith(".xml") or name_l.endswith(".p7m") or ".xml." in name_l):
                            continue
                        d = zf.read(name)
                        xml_bytes = extract_p7m(d) if name_l.endswith(".p7m") else d
                        add_attachment_candidate(attachments, name, xml_bytes)
            except Exception as e:
                log.warning(f"  ZIP non leggibile {fn}: {e}")

    attachments.sort(key=lambda x: (x[0], x[1].lower()))
    return [(fn, xb, kind) for _, fn, xb, kind in attachments]

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
    log.info("=== Avvio sync PEC -> GitHub (v12) ===")
    index, index_sha = gh_read("ceraldi_fatture_index.json", {"fatture": [], "lastSync": ""})
    processed, proc_sha = gh_read("processed_ids.json", [])

    corrette_tipo = 0
    for f in index.get("fatture", []):
        if f.get("tipo") == "ddt" and f.get("source") == "pec":
            f["tipo"] = "fattura"
            corrette_tipo += 1
    if corrette_tipo:
        log.info(f"  Corrette {corrette_tipo} fatture PEC con tipo 'ddt' → 'fattura'")

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

        def extract_folder_name(folder_item):
            folder_str = folder_item.decode() if isinstance(folder_item, bytes) else folder_item
            parts = folder_str.split('"')
            if len(parts) >= 2:
                candidate = parts[-2] if parts[-1].strip() == '' else parts[-1].strip()
            else:
                candidate = folder_str.split()[-1]
            return candidate.strip().strip('"')

        def select_folder_count(name):
            status, msgs = imap.select(f'"{name}"')
            if status != "OK":
                status, msgs = imap.select(name)
            if status != "OK":
                return None, None
            count = int(msgs[0].decode()) if msgs and msgs[0] else 0
            return status, count

        def folder_score(name, count):
            lname = name.lower()
            score = 0
            if any(token in lname for token in ("fatture ricevute", "fatturazione elettronica", "fatture", "fatturazione", "elettronica")):
                score += 100
            if any(token in lname for token in ("lette", "letti", "read", "archivio", "storico")):
                score -= 25
            if lname in ("inbox", "spam", "junk", "trash", "cestino", "bozze", "inviata", "sent", "drafts"):
                score -= 100
            score += min(count, 5000) / 1000.0
            return score

        folder_candidates = []
        for folder_item in (folders or []):
            fname = extract_folder_name(folder_item)
            if not fname or fname in ('', '.'):
                continue
            status, count = select_folder_count(fname)
            if status == "OK":
                score = folder_score(fname, count)
                folder_candidates.append((score, count, fname))
                log.info(f"  Cartella '{fname}': {count} email (score={score:.2f})")

        if folder_candidates:
            preferred = [c for c in folder_candidates if c[0] >= 50]
            chosen = max(preferred or folder_candidates, key=lambda item: (item[0], item[1]))
            cartella_selezionata = chosen[2]
        else:
            cartella_selezionata = 'INBOX'
            log.warning("Nessuna cartella interrogabile trovata, uso INBOX come fallback")

        status, msgs = imap.select(f'"{cartella_selezionata}"')
        if status != "OK":
            status, msgs = imap.select(cartella_selezionata)
        if status != "OK":
            log.error(f"Impossibile aprire la cartella '{cartella_selezionata}'")
            return

        log.info(f"Cartella selezionata: {cartella_selezionata} ({msgs[0].decode() if msgs and msgs[0] else '?'} email)")
        _, data = imap.search(None, "ALL")
        uids = data[0].split()
        log.info(f"Email trovate: {len(uids)}")

        for uid in uids:
            uid_str = f"{cartella_selezionata}:{uid.decode()}"
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
            invoice_candidates = [a for a in attachments if a[2] in ("invoice", "xml")]
            for fn, xml_bytes, kind in attachments:
                log.info(f"  Provo: {fn} ({len(xml_bytes)} bytes, kind={kind})")
                fattura = parse_xml(xml_bytes)
                if not fattura:
                    log.warning(f"  parse_xml fallito per {fn}")
                    continue

                chiave = f"{fattura['fornitore']}|{fattura['numero']}|{fattura['data']}"
                gia_esistente = None
                for f_idx in index.get("fatture", []):
                    if f"{f_idx.get('fornitore')}|{f_idx.get('numero')}|{f_idx.get('data','')}" == chiave:
                        gia_esistente = f_idx
                        break

                if gia_esistente:
                    if not gia_esistente.get("xmlGithubPath"):
                        log.info(f"  Fattura già in indice SENZA xml, tento upload: {chiave}")
                        xml_path = upload_xml_to_github(fattura, xml_bytes)
                        if xml_path:
                            gia_esistente["xmlGithubPath"] = xml_path
                            log.info("  ✓ xmlGithubPath popolato retroattivamente")
                        else:
                            xml_upload_failures += 1
                    else:
                        log.info(f"  Già in indice CON xml: {chiave}")
                    imported = True
                    break

                xml_path = upload_xml_to_github(fattura, xml_bytes)
                if not xml_path:
                    log.error(f"  ✗ Skip {fattura['fornitore']} n.{fattura['numero']}: upload XML fallito")
                    xml_upload_failures += 1
                    imported = False
                    break

                fattura["xmlGithubPath"] = xml_path
                index.setdefault("fatture", []).append(fattura)
                new_count += 1
                imported = True
                log.info(f"  + {fattura['fornitore']} n.{fattura['numero']} EUR {fattura['importo']} (xml: {xml_path})")
                break

            if imported or not invoice_candidates:
                processed.append(uid_str)
            else:
                log.warning(f"  {uid_str}: allegati fattura non importati (verrà ritentata)")

    index["lastSync"] = datetime.now(timezone.utc).isoformat()
    index["newCount"] = new_count

    gh_write(
        "ceraldi_fatture_index.json",
        index,
        f"Sync v12: {new_count} nuove, {corrette_tipo} tipo corretti, {recuperate} xml recuperati",
        index_sha,
    )
    gh_write("processed_ids.json", processed, "Aggiorna processed", proc_sha)

    log.info(f"=== Completata: {new_count} nuove, {corrette_tipo} tipo corretti, {recuperate} xml retroattivi ===")
    if xml_upload_failures:
        log.warning(f"⚠️ {xml_upload_failures} upload XML falliti — verranno ritentati alla prossima run")

if __name__ == "__main__":
    sync()
    log.info("=== GitHub Actions: sync completata (v12) ===")
