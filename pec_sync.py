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

# Supabase (opzionale — se non configurato, skip estrazione prezzi)
SB_URL = os.environ.get("SUPABASE_URL", "https://qaqqptpprmfjlolordaq.supabase.co")
SB_KEY = os.environ.get("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InFhcXFwdHBwcm1mamxvbG9yZGFxIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzU4NDQ3MDgsImV4cCI6MjA5MTQyMDcwOH0.kTnxsNY3tua_ya4LCB8-vkVdQ1QBPGtLL7Gfg121d1o")

# Mappa nomi fornitore dall'XML al nome normalizzato usato in ceraldi_ordini
# Chiave: parte del nome fornitore (lowercase), Valore: nome esatto nel catalogo
SUPPLIER_MAP = {
    "siro":        "Siro",
    "sud ingrosso":"Sud Ingrosso",
    "saima":       "Saima",
    "fiorentino":  "Fiorentino",
}

def normalizza_fornitore(nome_xml):
    """Normalizza il nome fornitore dall'XML al nome usato in ceraldi_ordini."""
    n = nome_xml.lower()
    for key, normalized in SUPPLIER_MAP.items():
        if key in n:
            return normalized
    return None  # fornitore non nella mappa → non elaborare


def estrai_prezzi_da_xml(xml_bytes, fattura_info):
    """
    Estrae le righe prezzo da un XML FatturaPA e le invia a Supabase.
    Ritorna il numero di righe inserite, o 0 se errore/skip.
    """
    try:
        root = ET.fromstring(xml_bytes)
    except Exception as e:
        log.warning(f"  [prezzi] XML non parsabile: {e}")
        return 0

    # Salta note di credito (non rappresentano prezzi di acquisto)
    NC_CODES = {"TD04", "TD05", "TD08", "TD24", "TD25"}
    tipo_doc = fattura_info.get("tipo", "")
    if tipo_doc == "nota_credito":
        log.info("  [prezzi] Nota di credito — skip")
        return 0

    # Controlla se il fornitore è nella mappa (abilitato per ceraldi_ordini)
    fornitore_norm = normalizza_fornitore(fattura_info.get("fornitore", ""))
    if not fornitore_norm:
        log.info(f"  [prezzi] Fornitore '{fattura_info.get('fornitore','')}' non nella mappa — skip")
        return 0

    # Helper per estrarre testo con/senza namespace
    ns = ""
    if root.tag.startswith("{"):
        ns = "{" + root.tag[1:root.tag.index("}")] + "}"

    def tx(path, ctx=None):
        el = (ctx or root).find(path.replace("/", f"/{ns}") if ns else path)
        return el.text.strip() if el is not None and el.text else ""

    def ga(tag, ctx=None):
        return list((ctx or root).iter(f"{ns}{tag}" if ns else tag))

    fattura_id = fattura_info.get("id", "")
    data_fatt  = fattura_info.get("data", "") or None

    righe = ga("DettaglioLinee")
    if not righe:
        log.info("  [prezzi] Nessuna DettaglioLinee")
        return 0

    rows_to_insert = []
    for r in righe:
        desc      = tx("Descrizione", r)
        cod_art   = tx("CodiceValore", r) or None
        try:
            qta   = float(tx("Quantita", r) or "1")
        except:
            qta   = 1.0
        try:
            p_unit = float(tx("PrezzoUnitario", r) or "0")
        except:
            p_unit = 0.0
        try:
            iva_pct = float(tx("AliquotaIVA", r) or "0")
        except:
            iva_pct = 0.0

        if not desc or p_unit <= 0:
            continue

        p_ivato = round(p_unit * (1 + iva_pct / 100), 4)

        rows_to_insert.append({
            "fattura_id":   fattura_id,
            "fornitore":    fornitore_norm,
            "data_fattura": data_fatt,
            "cod_articolo": cod_art,
            "descrizione":  desc,
            "qta":          qta,
            "prezzo_unit":  round(p_unit, 4),
            "iva":          iva_pct or None,
            "prezzo_ivato": p_ivato,
            "stato":        "pending"
        })

    if not rows_to_insert:
        log.info("  [prezzi] Nessuna riga valida estratta")
        return 0

    # Invia a Supabase con ignore-duplicates (safe se la fattura è già stata processata)
    try:
        body = json.dumps(rows_to_insert).encode()
        req = urllib.request.Request(
            f"{SB_URL}/rest/v1/prezzi_da_fatture",
            data=body,
            method="POST",
            headers={
                "apikey":        SB_KEY,
                "Authorization": f"Bearer {SB_KEY}",
                "Content-Type":  "application/json",
                "Prefer":        "resolution=ignore-duplicates,return=minimal"
            }
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            status = resp.status
        if status in (200, 201):
            log.info(f"  [prezzi] ✓ {len(rows_to_insert)} righe inserite per {fornitore_norm}")
            return len(rows_to_insert)
        else:
            log.warning(f"  [prezzi] Supabase HTTP {status}")
            return 0
    except Exception as e:
        log.warning(f"  [prezzi] Errore Supabase: {e}")
        return 0

def gh_read(path, default):
    req = urllib.request.Request(
        f"https://api.github.com/repos/{GH_REPO}/contents/{path}?ref={GH_BRANCH}",
        headers={"Authorization": f"token {GH_TOKEN}", "Accept": "application/vnd.github.v3+json", "User-Agent": "ceraldi"}
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
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
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read()).get("content", {}).get("sha")
    try:
        return _do_write(sha)
    except Exception as e:
        if "422" in str(e) or "409" in str(e):
            log.warning(f"  gh_write conflict su {path}, rileggo sha e riprovo")
            fresh_sha = gh_get_sha(path)
            return _do_write(fresh_sha)
        raise

def gh_write_raw(path, content_bytes, msg, sha=None, max_retries=3):
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
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read()).get("content", {}).get("sha")
        except Exception as e:
            last_exc = e
            err_str = str(e)
            if "409" in err_str or "422" in err_str:
                log.warning(f"  gh_write_raw conflict su {path}, attempt {attempt+1}/{max_retries}")
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
        with urllib.request.urlopen(req, timeout=20) as r:
            return json.loads(r.read()).get("sha")
    except:
        return None

def extract_p7m(data):
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
        tag = data[pos]; pos += 1
        length, pos = read_len(data, pos)
        is_constructed = bool(tag & 0x20)
        is_octet_type  = (tag & 0x1f) == 0x04
        if length == -1:
            if is_octet_type and not is_constructed:
                end_indef = data.find(b'\x00\x00', pos)
                if end_indef == -1: break
                content = data[pos:end_indef]; pos = end_indef + 2
                if not collecting:
                    for marker in XML_MARKERS:
                        mi = content.find(marker)
                        if mi != -1:
                            collecting = True; xml_chunks = [content[mi:]]; break
                else:
                    if is_xml_content(content): xml_chunks.append(content)
            continue
        if length <= 0: continue
        if pos + length > len(data): break
        content = data[pos:pos + length]
        if is_octet_type and not is_constructed:
            if not collecting:
                for marker in XML_MARKERS:
                    mi = content.find(marker)
                    if mi != -1:
                        collecting = True; xml_chunks = [content[mi:]]; break
            else:
                if is_xml_content(content): xml_chunks.append(content)
                else: collecting = False; xml_chunks = []
            if collecting and xml_chunks:
                combined = b''.join(xml_chunks)
                end_pos = find_xml_end(combined)
                if end_pos != -1:
                    candidate = combined[:end_pos]
                    try:
                        ET.fromstring(candidate)
                        log.info(f"  p7m: XML da {len(xml_chunks)} chunk (strategia 2)")
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
        cleaned = bytearray(b for b in raw if b >= 0x09)
        try:
            ET.fromstring(bytes(cleaned))
            log.info("  p7m: XML estratto con scansione bruta")
            return bytes(cleaned)
        except:
            pass

    log.warning("  p7m: impossibile estrarre XML")
    return None


def parse_xml(xml_bytes):
    try:
        root = ET.fromstring(xml_bytes)
    except Exception as e:
        log.warning(f"  ET error: {e}")
        return None

    ns = ""
    if root.tag.startswith("{"):
        ns = "{" + root.tag[1:root.tag.index("}")] + "}"

    if "FileMetadati" in root.tag.split("}")[-1]:
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
    # Cerca TipoDocumento con path XPath e come fallback con ricerca diretta per tag
    tipo_doc = tx("./FatturaElettronicaBody/DatiGenerali/DatiGeneraliDocumento/TipoDocumento")
    if not tipo_doc:
        # Fallback: cerca il tag TipoDocumento ovunque nell'albero XML (gestisce qualsiasi namespace)
        for el in root.iter():
            if el.tag.split("}")[-1] == "TipoDocumento" and el.text:
                tipo_doc = el.text.strip()
                break
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

    # Riconosce note credito (TD04, TD05, TD08, TD24, TD25)
    NC_CODES = {"TD04", "TD05", "TD08", "TD24", "TD25"}
    if tipo_doc in NC_CODES:
        tipo = "nota_credito"
    elif tipo_doc == "TD01" or not tipo_doc:
        tipo = "fattura"
    else:
        tipo = "fattura"

    # ID deterministico basato su contenuto (non su tempo)
    safe_forn_id = "".join(c if c.isalnum() else "_" for c in fornitore)[:30]
    safe_num_id  = "".join(c if c.isalnum() else "_" for c in numero)[:30]
    data_id = (data_raw[:10] if data_raw else "nodate").replace("-", "")
    deterministic_id = f"pec_{safe_forn_id}_{safe_num_id}_{data_id}"

    log.info(f"  Parsed OK: {fornitore} n.{numero} {data_raw} EUR {imp} tipo={tipo_doc or 'fattura'} pag={pag}")

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
        log.error(f"  ✗ Upload XML FALLITO: {xe}")
        return None


def sync():
    log.info("=== Avvio sync PEC -> GitHub (v14) ===")
    log.info(f"Ora UTC: {datetime.now(timezone.utc).isoformat()}")

    # ── Leggi indice e processed in modo atomico (rileggi SHA freschi) ──
    index, index_sha = gh_read("ceraldi_fatture_index.json", {"fatture": [], "lastSync": ""})
    processed, proc_sha = gh_read("processed_ids.json", [])

    # ── Costruisci set di dedup basato sull'indice (fonte di verità primaria) ──
    # FIX v13: il dedup NON si basa solo su processed_ids.json
    # ma anche su fornitore|numero|data già nell'indice.
    # Così anche se processed viene resettato, i duplicati non entrano.
    chiavi_indice = set()
    for f in index.get("fatture", []):
        chiave = f"{f.get('fornitore','')}|{f.get('numero','')}|{f.get('data','')}"
        chiavi_indice.add(chiave)

    # Set di processed per lookup veloce
    processed_set = set(processed)

    log.info(f"Indice: {len(chiavi_indice)} fatture. Processed: {len(processed_set)} email.")

    # ── Re-sync note credito e fatture mancanti ────────────────────────
    # Costruisci set degli ID deterministici presenti nell'indice
    ids_in_indice = set(f.get('id','') for f in index.get('fatture',[]))
    # Trova email in processed che non hanno più la fattura nell'indice
    # Questi potrebbero essere stati cancellati manualmente → rimetti in gioco
    # Nota: non possiamo sapere quale processed_key corrisponde a quale fattura
    # senza scaricare tutte le email. Però possiamo usare una strategia diversa:
    # mantieni un mapping processed_key → fattura_id in un file separato.
    # Per ora: se l'indice ha meno fatture del previsto, logga per debug.
    log.info(f"IDs in indice: {len(ids_in_indice)}")

    new_count = 0
    xml_upload_failures = 0
    nuove_fatture = []  # accumula le nuove prima del commit
    nuovi_processed = []  # accumula i nuovi processed prima del commit

    imap_timeout = 30  # secondi
    imaplib.IMAP4_SSL.port = PEC_PORT
    with imaplib.IMAP4_SSL(PEC_HOST, PEC_PORT) as imap:
        imap.socket().settimeout(imap_timeout)
        imap.login(PEC_USER, PEC_PASS)

        _, folders = imap.list()
        log.info("=== CARTELLE DISPONIBILI ===")
        folder_names = []
        for f in (folders or []):
            raw = f.decode() if isinstance(f, bytes) else f
            # Estrai solo il nome della cartella (ultima parte dopo lo spazio)
            parts = raw.split('"."')
            fname = parts[-1].strip().strip('"') if len(parts)>1 else raw
            folder_names.append(fname)
            log.info(f"  CARTELLA: {repr(fname)}")
        log.info(f"=== TOTALE {len(folder_names)} CARTELLE ===")

        # Lista completa nomi possibili — Aruba PEC usa nomi diversi in base alla configurazione
        _folder_candidates = [
            'Fatture ricevute',
            '"Fatture ricevute"',
            'INBOX.Fatture ricevute',
            '"INBOX.Fatture ricevute"',
            'Fatturazione Elettronica',
            '"Fatturazione Elettronica"',
            'INBOX.Fatturazione Elettronica',
            'FatturazioneElettronica',
            'Fatturazione',
            'INBOX',
        ]
        # Prova tutte le cartelle — usa la prima che ha email con allegati XML
        # Non fermarsi alla prima che risponde OK (INBOX risponde sempre OK ma ha email sbagliate)
        _selected_folder = None
        _selected_msgs = None
        for try_name in _folder_candidates:
            status, msgs = imap.select(try_name, readonly=True)
            if status != "OK":
                continue
            n = int(msgs[0].decode()) if msgs and msgs[0] else 0
            log.info(f"  Cartella '{try_name}': {n} email")
            if n > 0 and try_name != 'INBOX':
                _selected_folder = try_name
                _selected_msgs = msgs
                break
        # Se non trovata cartella specifica, usa INBOX come fallback
        if not _selected_folder:
            status, msgs = imap.select('INBOX')
            _selected_folder = 'INBOX'
            _selected_msgs = msgs
            log.warning("  Nessuna cartella fatture trovata — uso INBOX come fallback")
        # Ri-seleziona in modalità read-write
        status, msgs = imap.select(_selected_folder)
        try_name = _selected_folder
        log.info(f"Cartella selezionata: {try_name} ({msgs[0].decode()} email)")
        if True:  # mantieni indentazione del blocco originale

            log.info(f"Cartella selezionata: {try_name} ({msgs[0].decode()} email)")
            # Cerca solo email degli ultimi 30 giorni — evita di scansionare tutta la casella
            from datetime import timedelta
            since_date = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%d-%b-%Y")
            _, data = imap.search(None, f"SINCE {since_date}")
            uids_recent = data[0].split()
            # Se non trova email recenti, prova con 60 giorni
            if not uids_recent:
                since_date2 = (datetime.now(timezone.utc) - timedelta(days=60)).strftime("%d-%b-%Y")
                _, data = imap.search(None, f"SINCE {since_date2}")
                uids_recent = data[0].split()
            uids = uids_recent
            log.info(f"Email trovate (ultimi 30gg): {len(uids)}")

            for uid in uids:
                uid_bare = uid.decode()

                # FIX v13: usa Message-ID come chiave processed (stabile tra run)
                # Fallback a uid_bare se Message-ID non disponibile
                try:
                    _, hdr_data = imap.fetch(uid, "(BODY[HEADER.FIELDS (MESSAGE-ID)])")
                    msg_id_header = ""
                    if hdr_data and hdr_data[0]:
                        raw_hdr = hdr_data[0][1].decode("utf-8", errors="ignore") if isinstance(hdr_data[0][1], bytes) else ""
                        for line in raw_hdr.splitlines():
                            if line.lower().startswith("message-id:"):
                                msg_id_header = line.split(":", 1)[1].strip()
                                break
                    processed_key = msg_id_header if msg_id_header else f"{try_name}:{uid_bare}"
                except:
                    processed_key = f"{try_name}:{uid_bare}"

                # Controlla se già processata (con chiave nuova O con chiavi vecchie per compatibilità)
                old_key_1 = f"{try_name}:{uid_bare}"
                old_key_2 = uid_bare
                gia_in_processed = (processed_key in processed_set or
                                    old_key_1 in processed_set or
                                    old_key_2 in processed_set)

                if gia_in_processed:
                    # BUGFIX: anche se è in processed, controlla se la fattura è
                    # effettivamente nell'indice. Se non c'è, reimporta.
                    # Questo risolve il caso in cui processed viene aggiornato
                    # ma il commit dell'indice fallisce.
                    # Per efficienza: scarica solo l'header XML per vedere il numero fattura
                    try:
                        _, hdr_check = imap.fetch(uid, "(BODY[HEADER.FIELDS (SUBJECT FROM)])")
                        # Scarica il messaggio completo solo se serve il controllo
                        _, msg_data_check = imap.fetch(uid, "(RFC822)")
                        if msg_data_check and msg_data_check[0]:
                            msg_check = email.message_from_bytes(msg_data_check[0][1])
                            atts_check = get_attachments(msg_check)
                            for _, xml_bytes_check in atts_check:
                                fatt_check = parse_xml(xml_bytes_check)
                                if fatt_check:
                                    chiave_check = f"{fatt_check['fornitore']}|{fatt_check['numero']}|{fatt_check['data']}"
                                    if chiave_check not in chiavi_indice:
                                        log.warning(f"  Re-import: '{chiave_check}' era in processed ma non nell'indice")
                                        # Rimuovi da processed_set per forzare reimport
                                        processed_set.discard(processed_key)
                                        processed_set.discard(old_key_1)
                                        processed_set.discard(old_key_2)
                                        gia_in_processed = False
                                    break
                    except Exception as e_check:
                        log.debug(f"  Check indice fallito per {processed_key}: {e_check}")
                    if gia_in_processed:
                        continue

                try:
                    _, msg_data = imap.fetch(uid, "(RFC822)")
                    if not msg_data or not msg_data[0]:
                        nuovi_processed.append(processed_key)
                        processed_set.add(processed_key)
                        continue
                    msg = email.message_from_bytes(msg_data[0][1])
                except Exception as e:
                    log.warning(f"Fetch {processed_key}: {e}")
                    nuovi_processed.append(processed_key)
                    processed_set.add(processed_key)
                    continue

                attachments = get_attachments(msg)
                if not attachments:
                    log.info(f"  {processed_key}: nessun allegato XML")
                    nuovi_processed.append(processed_key)
                    processed_set.add(processed_key)
                    continue

                imported = False
                for fn, xml_bytes in attachments:
                    log.info(f"  Provo: {fn} ({len(xml_bytes)} bytes)")
                    fattura = parse_xml(xml_bytes)
                    if not fattura:
                        log.warning(f"  parse_xml fallito per {fn}")
                        continue

                    chiave = f"{fattura['fornitore']}|{fattura['numero']}|{fattura['data']}"

                    # FIX v13: dedup primario sull'indice, NON su processed
                    if chiave in chiavi_indice:
                        log.info(f"  Già in indice (dedup contenuto): {chiave}")
                        imported = True
                        break

                    # Upload XML prima di aggiungere all'indice
                    xml_path = upload_xml_to_github(fattura, xml_bytes)
                    if not xml_path:
                        log.error(f"  ✗ Skip {fattura['fornitore']} n.{fattura['numero']}: upload XML fallito")
                        xml_upload_failures += 1
                        imported = False
                        break

                    fattura["xmlGithubPath"] = xml_path
                    fattura["pecMsgId"] = processed_key  # salva l'id email per poter fare re-sync
                    nuove_fatture.append(fattura)
                    chiavi_indice.add(chiave)  # aggiorna il set locale per questa run
                    new_count += 1
                    imported = True
                    log.info(f"  + {fattura['fornitore']} n.{fattura['numero']} EUR {fattura['importo']} tipo={fattura.get('tipo','fattura')}")

                    # ── Estrai prezzi e invia a Supabase ──────────────────
                    try:
                        n_prezzi = estrai_prezzi_da_xml(xml_bytes, fattura)
                        if n_prezzi > 0:
                            log.info(f"  [prezzi] {n_prezzi} righe prezzo inviate a Supabase")
                    except Exception as ep:
                        log.warning(f"  [prezzi] Errore estrazione (non bloccante): {ep}")
                    # ─────────────────────────────────────────────────────

                    break

                if imported:
                    nuovi_processed.append(processed_key)
                    processed_set.add(processed_key)
                else:
                    log.warning(f"  {processed_key}: non importata, verrà ritentata")

            pass  # fine loop email

    # ── Commit atomico: rileggi SHA freschi prima di scrivere ──
    # FIX v13: rileggo SHA aggiornati ADESSO (non quelli letti all'inizio)
    # per evitare conflitti se il workflow è girato in parallelo.
    # ── Cleanup processed: se fatture cancellate dall'indice, togli da processed ──
    # Leggi indice fresco per vedere se ci sono fatture cancellate
    try:
        index_check, _ = gh_read("ceraldi_fatture_index.json", {"fatture": []})
        ids_attuali = set(f.get('id','') for f in index_check.get('fatture',[]))
        # Costruisci mapping pecMsgId → fatturaId dall'indice
        msg_to_id = {}
        for f in index_check.get('fatture',[]):
            if f.get('pecMsgId'):
                msg_to_id[f['pecMsgId']] = f['id']
        # Processed da rimuovere: email il cui ID fattura non è più nell'indice
        proc_da_rimuovere = set()
        for pk in list(processed_set):
            if pk in msg_to_id and msg_to_id[pk] not in ids_attuali:
                proc_da_rimuovere.add(pk)
                log.info(f"  Re-sync: rimozione da processed '{pk}' (fattura cancellata)")
        if proc_da_rimuovere:
            processed_set -= proc_da_rimuovere
            nuovi_processed_clean = [p for p in list(processed_set)]
            proc_fresh, proc_sha_fresh2 = gh_read("processed_ids.json", [])
            proc_set2 = set(proc_fresh) - proc_da_rimuovere
            gh_write("processed_ids.json", list(proc_set2),
                     f"Re-sync: rimossi {len(proc_da_rimuovere)} processed per fatture cancellate", proc_sha_fresh2)
            log.info(f"  Cleanup: {len(proc_da_rimuovere)} email rimesse in gioco per re-sync")
    except Exception as ec:
        log.warning(f"  Cleanup processed fallito (non bloccante): {ec}")

    if nuove_fatture or nuovi_processed:
        log.info(f"Commit: {len(nuove_fatture)} nuove fatture, {len(nuovi_processed)} nuovi processed")

        # Rileggi indice fresco per merge sicuro
        index_fresh, index_sha_fresh = gh_read("ceraldi_fatture_index.json", {"fatture": [], "lastSync": ""})
        # Aggiungi solo fatture non già presenti (doppio controllo)
        chiavi_fresh = set(
            f"{f.get('fornitore','')}|{f.get('numero','')}|{f.get('data','')}"
            for f in index_fresh.get("fatture", [])
        )
        fatture_da_aggiungere = [f for f in nuove_fatture
                                  if f"{f['fornitore']}|{f['numero']}|{f['data']}" not in chiavi_fresh]
        index_fresh.setdefault("fatture", []).extend(fatture_da_aggiungere)
        index_fresh["lastSync"] = datetime.now(timezone.utc).isoformat()
        index_fresh["newCount"] = new_count

        gh_write("ceraldi_fatture_index.json", index_fresh,
                 f"Sync v13: {len(fatture_da_aggiungere)} nuove fatture", index_sha_fresh)

        # Rileggi processed fresco per merge sicuro
        proc_fresh, proc_sha_fresh = gh_read("processed_ids.json", [])
        proc_set_fresh = set(proc_fresh)
        proc_set_fresh.update(nuovi_processed)
        gh_write("processed_ids.json", list(proc_set_fresh),
                 f"Sync v13: {len(nuovi_processed)} nuovi processed", proc_sha_fresh)

        log.info(f"✓ Commit completato: {len(fatture_da_aggiungere)} fatture aggiunte")
    else:
        # Nessuna nuova fattura ma aggiorna lastSync
        index["lastSync"] = datetime.now(timezone.utc).isoformat()
        index["newCount"] = 0
        gh_write("ceraldi_fatture_index.json", index,
                 "Sync v13: nessuna nuova fattura", index_sha)

    log.info(f"=== Completata: {new_count} nuove ===")
    if xml_upload_failures:
        log.warning(f"⚠️ {xml_upload_failures} upload XML falliti — verranno ritentati")


if __name__ == "__main__":
    import signal
    def _timeout_handler(signum, frame):
        log.error("=== TIMEOUT GLOBALE 300s — sync interrotta forzatamente ===")
        raise SystemExit(1)
    # Timeout di sicurezza: 5 minuti massimo per tutta la sync
    try:
        signal.signal(signal.SIGALRM, _timeout_handler)
        signal.alarm(300)
    except (AttributeError, OSError):
        pass  # Windows non supporta SIGALRM
    try:
        sync()
        log.info("=== GitHub Actions: sync completata ===")
    except SystemExit:
        log.error("=== Sync terminata per timeout ===")
        raise
    except Exception as e:
        log.error(f"=== Sync fallita con eccezione: {e} ===")
        raise
