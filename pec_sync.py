import imaplib, email, os, json, time, logging, zipfile, io, urllib.request, base64, re
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

# ──────────────────────────────────────────────────────────────────────────
#  GitHub helpers (invariati dalla v11)
# ──────────────────────────────────────────────────────────────────────────
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
            headers={"Authorization": f"token {GH_TOKEN}", "Accept": "application/vnd.github.v3+json",
                     "Content-Type": "application/json", "User-Agent": "ceraldi"}
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
    last_exc = None
    for attempt in range(max_retries):
        try:
            body = {"message": msg, "content": base64.b64encode(content_bytes).decode(), "branch": GH_BRANCH}
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
                log.warning(f"  gh_write_raw {err_str[:40]} su {path}, attempt {attempt+1}/{max_retries}")
                time.sleep(1.5 * (attempt + 1))
                sha = gh_get_sha(path)
                continue
            elif "403" in err_str or "rate limit" in err_str.lower():
                log.warning(f"  gh_write_raw rate limit su {path}, attendo 60s")
                time.sleep(60)
                continue
            else:
                raise
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

# ──────────────────────────────────────────────────────────────────────────
#  FIX v12 — extract_p7m completamente riscritto
#
#  Problema riscontrato (log del 19/04 in poi):
#    "p7m: strategie 1-2 fallite, provo scansione bruta (strategia 3)"
#    "p7m: impossibile estrarre XML con nessuna strategia"
#
#  Causa: nei nuovi p7m di Aruba l'XML è in chiaro all'inizio del payload
#  ma è preceduto/seguito da byte binari (header CMS + certificato/firma).
#  La vecchia strategia 1 trovava il marker `<?xml` e poi prendeva fino
#  all'ultima `</p:FatturaElettronica>`, ma il candidate veniva passato
#  ad ET.fromstring senza pulizia → ET fallisce sulla dichiarazione
#  encoding="utf-8" perché nel chunk possono restare byte non-utf8 prima
#  del primo `<` (es. BOM spuri, o per via del trim impreciso del marker).
#
#  Soluzione v12:
#   - cerco TUTTI i marker XML, prendo il primo offset valido
#   - per la fine cerco TUTTI i possibili tag di chiusura, prendo l'ULTIMO
#     (gestisce anche fatture multiple dentro lo stesso p7m → caso raro)
#   - prima di passare a ET.fromstring rimuovo eventuale BOM/byte non
#     stampabili a inizio, e provo prima il parse "as-is", poi con la
#     dichiarazione encoding rimossa (workaround per encoding dichiarato
#     non-ascii ma contenuto già unicode)
# ──────────────────────────────────────────────────────────────────────────

XML_MARKERS = (
    b"<?xml",
    b"<p:FatturaElettronica",
    b"<FatturaElettronica",
    b"<ns2:FatturaElettronica",
    b"<ns:FatturaElettronica",
)
XML_ENDS = (
    b"</p:FatturaElettronica>",
    b"</FatturaElettronica>",
    b"</ns2:FatturaElettronica>",
    b"</ns:FatturaElettronica>",
)

def _try_parse(xml_bytes):
    """Tenta vari modi di parsare l'XML. Ritorna i bytes puliti o None."""
    if not xml_bytes or len(xml_bytes) < 100:
        return None
    # 1) Pulisci BOM e byte non stampabili iniziali fino al primo '<'
    first_lt = xml_bytes.find(b'<')
    if first_lt > 0:
        xml_bytes = xml_bytes[first_lt:]
    # 2) Tronca eventuali byte spuri dopo l'ultimo '>'
    last_gt = xml_bytes.rfind(b'>')
    if last_gt != -1:
        xml_bytes = xml_bytes[:last_gt + 1]
    # Tentativo 1: parse diretto
    try:
        ET.fromstring(xml_bytes)
        return xml_bytes
    except Exception:
        pass
    # Tentativo 2: rimuovi la dichiarazione XML (a volte dichiara encoding sbagliato)
    try:
        no_decl = re.sub(rb'^\s*<\?xml[^>]*\?>', b'', xml_bytes)
        ET.fromstring(no_decl)
        return no_decl
    except Exception:
        pass
    # Tentativo 3: rimuovi caratteri di controllo non validi in XML 1.0
    try:
        cleaned = bytes(b for b in xml_bytes if b >= 0x20 or b in (0x09, 0x0a, 0x0d))
        ET.fromstring(cleaned)
        return cleaned
    except Exception:
        pass
    # Tentativo 4: decodifica come latin-1 (mai fallisce), riencoda utf-8, rimuovi decl
    try:
        as_text = xml_bytes.decode('latin-1', errors='replace')
        as_text = re.sub(r'^\s*<\?xml[^>]*\?>', '', as_text)
        # rimuovi caratteri di controllo non-XML
        as_text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', as_text)
        reenc = as_text.encode('utf-8')
        ET.fromstring(reenc)
        return reenc
    except Exception:
        pass
    return None


def extract_p7m(data):
    """
    Estrae l'XML della FatturaElettronica da un file .p7m (PKCS#7/CMS).
    Versione v12: più robusta, tenta più strategie e più cleanup.
    """
    if not data:
        return None

    # ── Strategia 1: XML in chiaro nel payload (caso più comune Aruba) ──
    # Trovo il PRIMO marker e l'ULTIMO end, poi pulisco e parso.
    best_start = -1
    for marker in XML_MARKERS:
        i = data.find(marker)
        if i != -1 and (best_start == -1 or i < best_start):
            best_start = i

    if best_start != -1:
        chunk = data[best_start:]
        best_end = -1
        for end in XML_ENDS:
            j = chunk.rfind(end)
            if j != -1:
                e = j + len(end)
                if e > best_end:
                    best_end = e
        if best_end != -1:
            candidate = chunk[:best_end]
            parsed = _try_parse(candidate)
            if parsed:
                log.info(f"  p7m: XML estratto in chiaro ({len(parsed)} bytes) [strategia 1 v12]")
                return parsed

    # ── Strategia 2: scansione ASN.1 OCTET STRING (per p7m che incapsulano davvero) ──
    def read_len(d, pos):
        if pos >= len(d): return 0, pos
        lb = d[pos]; pos += 1
        if lb < 0x80: return lb, pos
        if lb == 0x80: return -1, pos
        n = lb & 0x7f
        if n > 4 or pos + n > len(d): return 0, pos
        v = 0
        for _ in range(n):
            v = (v << 8) | d[pos]; pos += 1
        return v, pos

    pos = 0
    xml_chunks = []
    collecting = False
    while pos < len(data) - 4:
        tag = data[pos]; pos += 1
        length, pos = read_len(data, pos)
        is_constructed = bool(tag & 0x20)
        is_octet = (tag & 0x1f) == 0x04

        if length == -1:
            if is_octet and not is_constructed:
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
                    xml_chunks.append(content)
            continue

        if length <= 0: continue
        if pos + length > len(data): break
        content = data[pos:pos + length]

        if is_octet and not is_constructed:
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
                # cerca la fine
                for end in XML_ENDS:
                    j = combined.rfind(end)
                    if j != -1:
                        candidate = combined[:j + len(end)]
                        parsed = _try_parse(candidate)
                        if parsed:
                            log.info(f"  p7m: XML da {len(xml_chunks)} OCTET STRING [strategia 2 v12]")
                            return parsed
            pos += length
        elif is_constructed or tag in (0x30, 0x31, 0xa0, 0xa1, 0xa2, 0xa3):
            pass  # entra nel constructed
        else:
            pos += length

    # ── Strategia 3: scansione bruta su tutto il file ──
    log.warning("  p7m: strategie 1-2 fallite, scansione bruta (strategia 3 v12)")
    for marker in XML_MARKERS:
        i = data.find(marker)
        if i == -1: continue
        chunk = data[i:]
        for end in XML_ENDS:
            j = chunk.rfind(end)
            if j == -1: continue
            candidate = chunk[:j + len(end)]
            parsed = _try_parse(candidate)
            if parsed:
                log.info("  p7m: XML estratto con scansione bruta v12")
                return parsed

    log.warning("  p7m: impossibile estrarre XML con nessuna strategia")
    return None


# ──────────────────────────────────────────────────────────────────────────
#  parse_xml — invariato dalla v11, ma con miglior skip metadati/ricevute
# ──────────────────────────────────────────────────────────────────────────
def parse_xml(xml_bytes):
    try:
        root = ET.fromstring(xml_bytes)
    except Exception as e:
        log.warning(f"  ET error: {e} — raw: {xml_bytes[:200].decode('utf-8','ignore')}")
        return None

    tag_local = root.tag.split("}")[-1] if "}" in root.tag else root.tag

    # FIX v12: skip esplicito di file metadati e ricevute PEC/SDI
    if tag_local in ("FileMetadati", "Postacert", "DatiCertificazione", "Notifica",
                     "RicevutaConsegna", "RicevutaScarto", "NotificaEsito",
                     "NotificaDecorrenzaTermini", "NotificaMancataConsegna",
                     "AttestazioneTrasmissioneFattura"):
        log.info(f"  Skipping {tag_local} (non è una fattura)")
        return None

    if tag_local != "FatturaElettronica":
        log.info(f"  Skipping root tag '{tag_local}' (non è una FatturaElettronica)")
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
    except:
        imp = 0.0

    log.info(f"  Parsed OK: {fornitore} n.{numero} {data_raw} EUR {imp} pag={pag} numDdt={num_ddt or '—'}")

    safe_forn_id = "".join(c if c.isalnum() else "_" for c in fornitore)[:30]
    safe_num_id  = "".join(c if c.isalnum() else "_" for c in numero)[:30]
    data_id = (data_raw[:10] if data_raw else "nodate").replace("-", "")
    deterministic_id = f"pec_{safe_forn_id}_{safe_num_id}_{data_id}"

    return {
        "id": deterministic_id,
        "tipo": "fattura",
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


# ──────────────────────────────────────────────────────────────────────────
#  FIX v12 — get_attachments riscritto
#
#  Vecchio bug: il check
#      fn_l.endswith(".p7m") and ".xml" in fn_l
#  è OK per "IT123.xml.p7m" ma NON per "IT123.p7m" senza ".xml" nel nome.
#  Inoltre i .p7m che NON contengono "_MT_" potrebbero essere fatture
#  reali con nome non standard. Trattiamo TUTTI i .p7m come potenziali
#  fatture e filtriamo per contenuto, non per nome.
#
#  Inoltre: scartiamo da subito daticert.xml, smime.p7s e file metadati
#  SDI (con _MT_ nel nome).
# ──────────────────────────────────────────────────────────────────────────
def get_attachments(msg):
    attachments = []
    for part in msg.walk():
        fn = part.get_filename() or ""
        fn_l = fn.lower()
        if not fn_l:
            continue

        # Esclusioni dirette: ricevute PEC, firme S/MIME, smime
        if fn_l in ("daticert.xml", "smime.p7s", "smime.p7m", "postacert.eml"):
            log.info(f"  Skip allegato di sistema: {fn}")
            continue

        is_metadata = "_MT_" in fn.upper() or fn_l.endswith("_metadati.xml")
        xml_bytes = None
        source_label = ""

        if fn_l.endswith(".p7m"):
            raw = part.get_payload(decode=True)
            if raw:
                xml_bytes = extract_p7m(raw)
                source_label = "p7m"
        elif fn_l.endswith(".xml"):
            xml_bytes = part.get_payload(decode=True)
            source_label = "xml"
        elif fn_l.endswith(".zip"):
            raw = part.get_payload(decode=True)
            if raw:
                try:
                    with zipfile.ZipFile(io.BytesIO(raw)) as zf:
                        for name in zf.namelist():
                            nl = name.lower()
                            if nl in ("daticert.xml",) or "_MT_" in name.upper():
                                continue
                            if nl.endswith(".p7m"):
                                d = zf.read(name)
                                xml_bytes = extract_p7m(d)
                                fn = name
                                source_label = "zip+p7m"
                                break
                            if nl.endswith(".xml"):
                                xml_bytes = zf.read(name)
                                fn = name
                                source_label = "zip+xml"
                                break
                except Exception as e:
                    log.warning(f"  zip {fn}: {e}")

        if not xml_bytes or len(xml_bytes) < 100:
            continue

        # Priorità: p7m fattura (0) > xml fattura (1) > metadati (9, scartati a valle)
        if is_metadata:
            priority = 9
        elif source_label.startswith("p7m") or source_label == "zip+p7m":
            priority = 0
        else:
            priority = 1
        attachments.append((priority, fn, xml_bytes, source_label))

    # Scarta i metadati (li teniamo solo come fallback se non c'è altro)
    fatture_candidate = [a for a in attachments if a[0] < 9]
    if fatture_candidate:
        attachments = fatture_candidate

    attachments.sort(key=lambda x: x[0])
    return [(fn, xb, src) for _, fn, xb, src in attachments]


# ──────────────────────────────────────────────────────────────────────────
def upload_xml_to_github(fattura, xml_bytes):
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
    recuperate = 0
    for f in index.get("fatture", []):
        if f.get("xmlGithubPath"):
            continue
        if not f.get("fornitore") or not f.get("numero"):
            continue
        safe_forn = "".join(c if c.isalnum() or c in "-_." else "_" for c in f["fornitore"])[:40]
        safe_num  = "".join(c if c.isalnum() or c in "-_." else "_" for c in f["numero"])[:30]
        expected_path = f"fatture_xml/{safe_forn}_{safe_num}.xml"
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

        cartella_selezionata = None
        max_email = 0
        nomi_da_provare = [
            'INBOX.Fatture ricevute', '"INBOX.Fatture ricevute"',
            'Fatture ricevute', '"Fatture ricevute"',
            '"Fatturazione Elettronica"', 'Fatturazione Elettronica',
            'INBOX.Fatturazione Elettronica', 'INBOX.Fatture',
            '"Fatture"', 'Fatture', 'INBOX.fatture', 'INBOX.fatturazione'
        ]
        for try_name in nomi_da_provare:
            status, msgs = imap.select(try_name)
            if status == "OK":
                n = int(msgs[0].decode()) if msgs and msgs[0] else 0
                log.info(f"  Trovata cartella '{try_name}' con {n} email")
                if n > max_email:
                    max_email = n
                    cartella_selezionata = try_name

        if not cartella_selezionata or max_email == 0:
            log.info("Nessuna cartella canonica trovata, scansiono tutte le cartelle...")
            _, folder_list = imap.list()
            for folder_item in (folder_list or []):
                folder_str = folder_item.decode() if isinstance(folder_item, bytes) else folder_item
                parts = folder_str.split('"')
                if len(parts) >= 2:
                    fname = parts[-2] if parts[-1].strip() == '' else parts[-1].strip()
                else:
                    fname = folder_str.split()[-1]
                fname = fname.strip().strip('"')
                if not fname or fname in ('', '.'):
                    continue
                status2, msgs2 = imap.select(f'"{fname}"')
                if status2 != "OK":
                    status2, msgs2 = imap.select(fname)
                if status2 == "OK":
                    n = int(msgs2[0].decode()) if msgs2 and msgs2[0] else 0
                    log.info(f"  Cartella '{fname}': {n} email")
                    if n > max_email and fname.upper() not in ('INBOX', 'SPAM', 'JUNK', 'TRASH', 'CESTINO', 'BOZZE', 'INVIATA', 'SENT', 'DRAFTS'):
                        max_email = n
                        cartella_selezionata = f'"{fname}"'

        if not cartella_selezionata:
            cartella_selezionata = 'INBOX'
            log.warning("Nessuna cartella fatture trovata, uso INBOX come fallback")

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
                log.info(f"  {uid_str}: nessun allegato fattura trovato")
                processed.append(uid_str)
                continue

            imported = False
            parse_attempts = 0
            for fn, xml_bytes, src in attachments:
                log.info(f"  Provo: {fn} ({len(xml_bytes)} bytes, {src})")
                fattura = parse_xml(xml_bytes)
                if not fattura:
                    parse_attempts += 1
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
                            log.info(f"  ✓ xmlGithubPath popolato retroattivamente")
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

            if imported:
                processed.append(uid_str)
            else:
                # FIX v12: se TUTTI gli allegati sono stati processati e nessuno
                # è una fattura valida (es. solo ricevute/metadati), marca come
                # processata per non ritentarla all'infinito.
                if parse_attempts == len(attachments):
                    log.info(f"  {uid_str}: nessun allegato è una fattura valida, marco come processata")
                    processed.append(uid_str)
                else:
                    log.warning(f"  {uid_str}: non importata (verrà ritentata)")

    index["lastSync"] = datetime.now(timezone.utc).isoformat()
    index["newCount"] = new_count

    gh_write("ceraldi_fatture_index.json", index,
             f"Sync v12: {new_count} nuove, {corrette_tipo} tipo corretti, {recuperate} xml recuperati", index_sha)
    gh_write("processed_ids.json", processed, "Aggiorna processed", proc_sha)

    log.info(f"=== Completata: {new_count} nuove, {corrette_tipo} tipo corretti, {recuperate} xml retroattivi ===")
    if xml_upload_failures:
        log.warning(f"⚠️ {xml_upload_failures} upload XML falliti — verranno ritentati alla prossima run")


if __name__ == "__main__":
    sync()
    log.info("=== GitHub Actions: sync completata ===")
