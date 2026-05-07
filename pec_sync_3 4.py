"""
Ceraldi PEC Sync v15 - Architettura Supabase-first

CAMBIAMENTI RISPETTO A v13:
- ELIMINATO processed_ids.json (causa principale dei bug ricorrenti)
- ELIMINATO ceraldi_fatture_index.json
- Supabase è ora UNICA fonte di verità per le fatture
- Dedup atomico via UNIQUE constraint su pec_id
- Delta sync nativo: 1 SELECT iniziale, poi solo INSERT delle nuove
- Nessun race condition possibile
- IMAP SEARCH SINCE per scaricare solo email recenti

REQUISITI SUPABASE:
- Tabella `fatture` con colonna `pec_id` (text)
- UNIQUE INDEX su pec_id WHERE pec_id IS NOT NULL (vedi migration_pec.sql)

VARIABILI D'AMBIENTE:
- PEC_USER, PEC_PASS: credenziali Aruba PEC
- GH_TOKEN, GH_REPO: per upload XML
- SUPABASE_URL, SUPABASE_KEY: per dedup e insert fatture
"""

import imaplib, email, os, json, time, logging, zipfile, io, base64, signal
import urllib.request, urllib.error, urllib.parse
from datetime import datetime, timezone, timedelta
from xml.etree import ElementTree as ET

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("ceraldi-pec")

# ── Env ──
PEC_HOST = os.environ.get("PEC_HOST", "imaps.pec.aruba.it")
PEC_PORT = int(os.environ.get("PEC_PORT", "993"))
PEC_USER = os.environ["PEC_USER"]
PEC_PASS = os.environ["PEC_PASS"]
GH_TOKEN = os.environ["GH_TOKEN"]
GH_REPO  = os.environ["GH_REPO"]
GH_BRANCH = os.environ.get("GH_BRANCH", "main")

SB_URL = os.environ.get("SUPABASE_URL", "https://qaqqptpprmfjlolordaq.supabase.co")
SB_KEY = os.environ.get("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InFhcXFwdHBwcm1mamxvbG9yZGFxIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzU4NDQ3MDgsImV4cCI6MjA5MTQyMDcwOH0.kTnxsNY3tua_ya4LCB8-vkVdQ1QBPGtLL7Gfg121d1o")

SB_HEADERS = {
    "apikey": SB_KEY,
    "Authorization": f"Bearer {SB_KEY}",
    "Content-Type": "application/json",
}

# ── Mappa codici TipoDocumento SDI ──
# Solo questi sono "acquisti" per cui estrarre prezzi
ACQUISTO_TD_CODES = {"TD01", "TD06", "TD07", "TD24", "TD25", "TD28"}
# Tipo da salvare in Supabase per ogni TD
TD_TO_TIPO = {
    "TD01": "fattura", "TD06": "fattura", "TD07": "fattura", "TD28": "fattura",
    "TD24": "fattura_differita", "TD25": "fattura_differita",
    "TD02": "fattura", "TD03": "fattura", "TD26": "fattura", "TD27": "fattura",
    "TD04": "nota_credito", "TD08": "nota_credito",
    "TD05": "nota_debito",  "TD09": "nota_debito",
    "TD16": "autofattura", "TD17": "autofattura", "TD18": "autofattura",
    "TD19": "autofattura", "TD20": "autofattura", "TD21": "autofattura",
    "TD22": "autofattura", "TD23": "autofattura", "TD29": "autofattura",
}

# Mappa fornitori per estrazione prezzi (compatibilità con ceraldi_ordini)
SUPPLIER_MAP = {
    "siro": "Siro", "sud ingrosso": "Sud Ingrosso",
    "saima": "Saima", "fiorentino": "Fiorentino",
}


# ──────────────────────────────────────────────────────────────────────
# SUPABASE HELPERS
# ──────────────────────────────────────────────────────────────────────

def sb_request(path, method="GET", body=None, headers_extra=None, timeout=20):
    """Chiamata REST Supabase con timeout."""
    url = f"{SB_URL}/rest/v1/{path}"
    h = dict(SB_HEADERS)
    if headers_extra:
        h.update(headers_extra)
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers=h)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            txt = r.read().decode()
            return r.status, json.loads(txt) if txt else None
    except urllib.error.HTTPError as e:
        err_body = e.read().decode() if hasattr(e, 'read') else str(e)
        return e.code, err_body


def sb_get_existing_pec_ids():
    """Scarica TUTTI i pec_id già presenti in Supabase. Una sola query.
       Ritorna un set per lookup O(1)."""
    log.info("Supabase: scarico set pec_id esistenti...")
    all_ids = set()
    offset = 0
    page_size = 1000
    while True:
        # Range header per paginazione
        h = {"Range": f"{offset}-{offset+page_size-1}", "Range-Unit": "items"}
        status, data = sb_request(
            "fatture?select=pec_id&pec_id=not.is.null",
            headers_extra=h
        )
        if status not in (200, 206) or not data:
            break
        for row in data:
            if row.get("pec_id"):
                all_ids.add(row["pec_id"])
        if len(data) < page_size:
            break
        offset += page_size
    log.info(f"Supabase: {len(all_ids)} pec_id già presenti")
    return all_ids


def sb_find_manual_invoice(fornitore, numero):
    """Cerca una fattura manuale esistente con stesso fornitore e numero.
       Se trovata, ritorna il suo id (per UPDATE invece di INSERT)."""
    if not fornitore or not numero:
        return None
    forn_norm = fornitore.lower().strip()
    num_norm = numero.strip()
    # ilike per case-insensitive
    q = f"fatture?select=id,pec_id&fornitore=ilike.{urllib.parse.quote(forn_norm)}&numero=eq.{urllib.parse.quote(num_norm)}&pec_id=is.null&limit=1"
    status, data = sb_request(q)
    if status == 200 and data and len(data) > 0:
        return data[0]["id"]
    return None


def sb_insert_fattura(fattura_payload):
    """INSERT in fatture. Usa Prefer: resolution=ignore-duplicates per
       gestire automaticamente dedup su pec_id UNIQUE."""
    h = {"Prefer": "return=representation,resolution=ignore-duplicates"}
    status, data = sb_request("fatture", method="POST", body=[fattura_payload], headers_extra=h)
    if status in (200, 201):
        if data and len(data) > 0:
            return data[0].get("id")
        # 201 senza body = già esistente (ignored)
        return "exists"
    log.error(f"INSERT fattura fallito {status}: {data}")
    return None


def sb_update_fattura(fattura_id, payload):
    """UPDATE fattura esistente (caso match con manuale)."""
    h = {"Prefer": "return=minimal"}
    status, data = sb_request(
        f"fatture?id=eq.{fattura_id}",
        method="PATCH", body=payload, headers_extra=h
    )
    return status in (200, 204)


def sb_insert_prezzi(rows):
    """INSERT batch in prezzi_da_fatture, ignora duplicati."""
    if not rows:
        return 0
    h = {"Prefer": "resolution=ignore-duplicates,return=minimal"}
    status, data = sb_request("prezzi_da_fatture", method="POST", body=rows, headers_extra=h)
    return len(rows) if status in (200, 201) else 0


# ──────────────────────────────────────────────────────────────────────
# GITHUB HELPERS (solo per upload XML)
# ──────────────────────────────────────────────────────────────────────

def gh_get_sha(path):
    req = urllib.request.Request(
        f"https://api.github.com/repos/{GH_REPO}/contents/{path}?ref={GH_BRANCH}",
        headers={"Authorization": f"token {GH_TOKEN}", "Accept": "application/vnd.github.v3+json", "User-Agent": "ceraldi"}
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read()).get("sha")
    except:
        return None


def gh_upload_xml(path, content_bytes, msg, max_retries=3):
    """Upload file XML su GitHub con retry per conflitti."""
    sha = gh_get_sha(path)
    last_exc = None
    for attempt in range(max_retries):
        try:
            body = {
                "message": msg,
                "content": base64.b64encode(content_bytes).decode(),
                "branch": GH_BRANCH,
            }
            if sha:
                body["sha"] = sha
            req = urllib.request.Request(
                f"https://api.github.com/repos/{GH_REPO}/contents/{path}",
                data=json.dumps(body).encode(), method="PUT",
                headers={
                    "Authorization": f"token {GH_TOKEN}",
                    "Accept": "application/vnd.github.v3+json",
                    "Content-Type": "application/json",
                    "User-Agent": "ceraldi",
                }
            )
            with urllib.request.urlopen(req, timeout=30) as r:
                return True
        except Exception as e:
            last_exc = e
            err = str(e)
            if "409" in err or "422" in err:
                time.sleep(1.5 * (attempt + 1))
                sha = gh_get_sha(path)
            elif "403" in err or "rate limit" in err.lower():
                log.warning(f"GitHub rate limit, attendo 60s")
                time.sleep(60)
            else:
                break
    log.error(f"Upload XML fallito: {last_exc}")
    return False


# ──────────────────────────────────────────────────────────────────────
# XML PARSING (riuso codice esistente)
# ──────────────────────────────────────────────────────────────────────

def extract_p7m(data):
    """Estrae XML da file P7M (firma PKCS#7)."""
    def read_len(data, pos):
        if pos >= len(data): return 0, pos
        lb = data[pos]; pos += 1
        if lb < 0x80: return lb, pos
        elif lb == 0x81: return data[pos], pos + 1
        elif lb == 0x82: return (data[pos] << 8) | data[pos+1], pos + 2
        elif lb == 0x83: return (data[pos] << 16) | (data[pos+1] << 8) | data[pos+2], pos + 3
        else: return (data[pos] << 24) | (data[pos+1] << 16) | (data[pos+2] << 8) | data[pos+3], pos + 4
    try:
        i = 0
        while i < len(data):
            tag = data[i]; i += 1
            length, i = read_len(data, i)
            if tag == 0x30:
                continue
            elif tag in (0x04, 0xa0):
                if tag == 0xa0:
                    if i < len(data) and data[i] == 0x04:
                        i += 1
                        length, i = read_len(data, i)
                content = data[i:i+length]
                if b"<?xml" in content[:200] or b"<FatturaElettronica" in content[:500] or b"<p:FatturaElettronica" in content[:500]:
                    return content
                i += length
            else:
                i += length
    except Exception as e:
        log.warning(f"  P7M parse error: {e}")
    # Fallback: cerca XML embedded grezzo
    for marker in (b"<?xml", b"<FatturaElettronica", b"<p:FatturaElettronica", b"<ns2:FatturaElettronica"):
        idx = data.find(marker)
        if idx >= 0:
            for end_marker in (b"</FatturaElettronica>", b"</p:FatturaElettronica>", b"</ns2:FatturaElettronica>"):
                end = data.find(end_marker, idx)
                if end >= 0:
                    return data[idx:end + len(end_marker)]
    return None


def parse_xml(xml_bytes):
    """Parsa un XML FatturaPA. Ritorna dict con dati o None se non valido/non importabile."""
    try:
        root = ET.fromstring(xml_bytes)
    except Exception as e:
        log.warning(f"  XML parse error: {e}")
        return None

    ns = ""
    if root.tag.startswith("{"):
        ns = "{" + root.tag[1:root.tag.index("}")] + "}"

    if "FileMetadati" in root.tag.split("}")[-1]:
        return None  # File metadati, non fattura

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
    numero = tx("./FatturaElettronicaBody/DatiGenerali/DatiGeneraliDocumento/Numero")
    data_raw = tx("./FatturaElettronicaBody/DatiGenerali/DatiGeneraliDocumento/Data")
    importo = tx("./FatturaElettronicaBody/DatiGenerali/DatiGeneraliDocumento/ImportoTotaleDocumento")
    iva_iban = tx("./FatturaElettronicaBody/DatiPagamento/DettaglioPagamento/IBAN")
    scadenza = tx("./FatturaElettronicaBody/DatiPagamento/DettaglioPagamento/DataScadenzaPagamento")
    mod_pag = tx("./FatturaElettronicaBody/DatiPagamento/DettaglioPagamento/ModalitaPagamento")
    pag_map = {"MP01": "contanti", "MP02": "assegno", "MP05": "bonifico"}
    num_ddt = tx("./FatturaElettronicaBody/DatiGenerali/DatiDDT/NumeroDDT")
    tipo_doc = tx("./FatturaElettronicaBody/DatiGenerali/DatiGeneraliDocumento/TipoDocumento")
    if not tipo_doc:
        # Fallback senza namespace
        for el in root.iter():
            if el.tag.split("}")[-1] == "TipoDocumento" and el.text:
                tipo_doc = el.text.strip()
                break

    if not fornitore or not numero:
        log.warning(f"  XML incompleto: fornitore='{fornitore}' numero='{numero}'")
        return None

    try:
        imp = float(str(importo).replace(",", "."))
    except:
        imp = 0.0

    tipo = TD_TO_TIPO.get(tipo_doc, "fattura")

    return {
        "tipo": tipo,
        "tipo_documento": tipo_doc or "TD01",
        "fornitore": fornitore,
        "numero": numero,
        "data": data_raw[:10] if data_raw else "",
        "importo": imp,
        "scadenza": scadenza[:10] if scadenza else "",
        "pagamento": pag_map.get(mod_pag, ""),
        "bon_iban": iva_iban,
        "num_ddt": num_ddt,
    }


def get_attachments(msg):
    """Estrae allegati XML da un messaggio email PEC."""
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


def estrai_prezzi_da_xml(xml_bytes, fattura_info, pec_id):
    """Estrae righe prezzo dall'XML e le inserisce in prezzi_da_fatture.
       Solo se TipoDocumento è un acquisto. Ritorna n. righe inserite."""
    tipo_doc = fattura_info.get("tipo_documento", "")
    if tipo_doc not in ACQUISTO_TD_CODES:
        return 0

    fornitore = fattura_info.get("fornitore", "")
    if not fornitore:
        return 0

    try:
        root = ET.fromstring(xml_bytes)
    except:
        return 0

    ns = ""
    if root.tag.startswith("{"):
        ns = "{" + root.tag[1:root.tag.index("}")] + "}"

    righe = []
    data_fatt = fattura_info.get("data") or None
    for linea in root.iter():
        if linea.tag.split("}")[-1] != "DettaglioLinee":
            continue

        def gx(tag):
            for el in linea.iter():
                if el.tag.split("}")[-1] == tag and el.text:
                    return el.text.strip()
            return ""

        desc = gx("Descrizione")
        if not desc:
            continue
        try:
            p_unit = float((gx("PrezzoUnitario") or "0").replace(",", "."))
        except:
            p_unit = 0.0
        if p_unit <= 0:
            continue
        try:
            qta = float((gx("Quantita") or "1").replace(",", "."))
        except:
            qta = 1.0
        try:
            iva_pct = float((gx("AliquotaIVA") or "0").replace(",", "."))
        except:
            iva_pct = 0.0

        cod_art = gx("CodiceValore") or None
        p_ivato = round(p_unit * (1 + iva_pct / 100), 4)

        righe.append({
            "fattura_id": pec_id,  # collega tramite pec_id
            "fornitore": fornitore,
            "data_fattura": data_fatt,
            "cod_articolo": cod_art,
            "descrizione": desc,
            "qta": qta,
            "prezzo_unit": round(p_unit, 4),
            "iva": iva_pct or None,
            "prezzo_ivato": p_ivato,
            "stato": "pending",
        })

    return sb_insert_prezzi(righe)


# ──────────────────────────────────────────────────────────────────────
# CORE: SYNC LOOP
# ──────────────────────────────────────────────────────────────────────

def safe_filename(s, maxlen=40):
    return "".join(c if c.isalnum() or c in "-_." else "_" for c in s)[:maxlen]


def deterministic_pec_id(fornitore, numero, data):
    """ID deterministico per dedup cross-device."""
    safe_forn = safe_filename(fornitore, 30)
    safe_num = safe_filename(numero, 30)
    data_id = (data[:10] if data else "nodate").replace("-", "")
    return f"pec_{safe_forn}_{safe_num}_{data_id}"


def select_best_folder(imap):
    """Seleziona la cartella PEC corretta provando vari nomi noti per Aruba.
       Usa quella con il maggior numero di email."""
    candidates = [
        '"Fatture ricevute"', 'Fatture ricevute',
        'INBOX.Fatture ricevute', '"INBOX.Fatture ricevute"',
        '"Fatturazione Elettronica"', 'Fatturazione Elettronica',
        'INBOX.Fatturazione Elettronica',
        'INBOX',
    ]
    best_name = None
    best_count = -1
    for name in candidates:
        try:
            status, msgs = imap.select(name, readonly=False)
            if status != "OK":
                continue
            n = int(msgs[0].decode()) if msgs and msgs[0] else 0
            log.info(f"  Cartella '{name}': {n} email")
            # Preferisci cartelle specifiche (non INBOX) anche se hanno meno email
            if name != "INBOX" and n > 0:
                return name, n
            if n > best_count:
                best_name = name
                best_count = n
        except Exception as e:
            log.debug(f"  Skip cartella {name}: {e}")
    return best_name, best_count


def fetch_message_id(imap, uid):
    """Estrae solo il Message-ID dall'header (fetch leggero)."""
    try:
        _, hdr = imap.fetch(uid, "(BODY.PEEK[HEADER.FIELDS (MESSAGE-ID)])")
        if hdr and hdr[0] and len(hdr[0]) > 1:
            raw = hdr[0][1].decode("utf-8", errors="ignore") if isinstance(hdr[0][1], bytes) else ""
            for line in raw.splitlines():
                if line.lower().startswith("message-id:"):
                    return line.split(":", 1)[1].strip()
    except Exception as e:
        log.debug(f"  Header fetch fail uid={uid}: {e}")
    return None


def sync():
    log.info("=== Avvio sync PEC v15 (Supabase-first) ===")
    log.info(f"Ora UTC: {datetime.now(timezone.utc).isoformat()}")

    # ── STEP 1: Delta sync — scarica tutti i pec_id già in Supabase ──
    try:
        existing_pec_ids = sb_get_existing_pec_ids()
    except Exception as e:
        log.error(f"Supabase irraggiungibile: {e}")
        return

    new_count = 0
    skipped_existing = 0
    failed_count = 0

    # ── STEP 2: Connetti IMAP ──
    imap = imaplib.IMAP4_SSL(PEC_HOST, PEC_PORT)
    imap.socket().settimeout(60)
    try:
        imap.login(PEC_USER, PEC_PASS)

        # Log cartelle disponibili (debug)
        _, folders = imap.list()
        log.info("=== CARTELLE PEC ===")
        for f in (folders or [])[:20]:
            try:
                raw = f.decode() if isinstance(f, bytes) else f
                log.info(f"  {raw}")
            except:
                pass

        folder_name, folder_count = select_best_folder(imap)
        if not folder_name:
            log.error("Nessuna cartella PEC accessibile")
            return
        log.info(f"=== Uso cartella: {folder_name} ({folder_count} email) ===")

        # ── STEP 3: SEARCH SINCE ultimi 30 giorni ──
        # Limita a email recenti per non scansionare tutta la casella
        since_date = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%d-%b-%Y")
        _, data = imap.search(None, f"SINCE {since_date}")
        uids = data[0].split() if data and data[0] else []

        if not uids:
            log.info("Nessuna email negli ultimi 30 giorni — provo ultimi 90 giorni")
            since_date2 = (datetime.now(timezone.utc) - timedelta(days=90)).strftime("%d-%b-%Y")
            _, data = imap.search(None, f"SINCE {since_date2}")
            uids = data[0].split() if data and data[0] else []

        log.info(f"Email da analizzare: {len(uids)}")

        # ── STEP 4: per ogni email ──
        for uid in uids:
            uid_bare = uid.decode()

            # 4a. Fetch leggero solo del Message-ID
            msg_id = fetch_message_id(imap, uid)
            if not msg_id:
                log.warning(f"  uid={uid_bare}: Message-ID mancante, skip")
                continue

            # 4b. Pre-check: il pec_id che genereremmo è già in Supabase?
            # Non possiamo saperlo senza parsare l'XML, ma possiamo usare
            # il Message-ID come marker temporaneo per skip rapido
            # Se esiste un pec_id che inizia con "msg_<msg_id>" → già fatto
            # Per ora controlliamo solo dopo aver parsato

            # 4c. Fetch completo email
            try:
                _, msg_data = imap.fetch(uid, "(RFC822)")
                if not msg_data or not msg_data[0]:
                    log.warning(f"  uid={uid_bare}: fetch vuoto, skip (riproverà)")
                    continue
                msg = email.message_from_bytes(msg_data[0][1])
            except Exception as e:
                log.warning(f"  uid={uid_bare} fetch fallito: {e} — skip (riproverà)")
                continue

            # 4d. Estrai allegati XML
            attachments = get_attachments(msg)
            if not attachments:
                log.debug(f"  uid={uid_bare}: nessun XML (notifica PEC)")
                continue

            # 4e. Parse XML e import
            imported_this_email = False
            for fn, xml_bytes in attachments:
                fattura = parse_xml(xml_bytes)
                if not fattura:
                    continue

                # Genera pec_id deterministico
                pec_id = deterministic_pec_id(
                    fattura["fornitore"], fattura["numero"], fattura["data"]
                )

                # DEDUP: pec_id già in Supabase → skip
                if pec_id in existing_pec_ids:
                    log.debug(f"  DEDUP skip: {pec_id}")
                    skipped_existing += 1
                    imported_this_email = True
                    break

                # Upload XML su GitHub
                xml_path = f"fatture_xml/{safe_filename(fattura['fornitore'])}_{safe_filename(fattura['numero'], 30)}.xml"
                if not gh_upload_xml(xml_path, xml_bytes,
                                      f"Fattura {fattura['fornitore']} n.{fattura['numero']}"):
                    log.error(f"  Upload XML fallito per {pec_id} — riproverà al prossimo run")
                    failed_count += 1
                    break  # non marcare come imported, riprova al prossimo run

                # Cerca match con fattura manuale esistente
                manual_id = sb_find_manual_invoice(fattura["fornitore"], fattura["numero"])

                # Costruisci payload Supabase (mappa snake_case)
                now_iso = datetime.now(timezone.utc).isoformat()
                payload = {
                    "tipo": fattura["tipo"],
                    "fornitore": fattura["fornitore"],
                    "numero": fattura["numero"],
                    "data": fattura["data"] or None,
                    "importo": fattura["importo"],
                    "pagamento": fattura["pagamento"],
                    "scadenza": fattura["scadenza"] or None,
                    "bon_iban": fattura["bon_iban"],
                    "num_ddt": fattura["num_ddt"],
                    "stato": "da_pagare",
                    "note": f"Importato da PEC ({datetime.now(timezone.utc).strftime('%d/%m/%Y')})",
                    "source": "pec",
                    "pec_id": pec_id,
                    "pec_verificata": True,
                    "pec_verificata_at": now_iso,
                    "xml_github_path": xml_path,
                    "fattura_allegata_name": xml_path.split("/")[-1],
                    "ts": int(time.time() * 1000),
                }

                if manual_id:
                    # UPDATE fattura manuale esistente
                    log.info(f"  ↺ UPDATE fattura manuale {manual_id}: {fattura['fornitore']} n.{fattura['numero']}")
                    # Non sovrascrivere stato/pagamento se l'utente li ha già impostati
                    update_payload = {
                        "tipo": payload["tipo"],
                        "data": payload["data"],
                        "importo": payload["importo"],
                        "scadenza": payload["scadenza"],
                        "bon_iban": payload["bon_iban"],
                        "num_ddt": payload["num_ddt"],
                        "source": "pec",
                        "pec_id": pec_id,
                        "pec_verificata": True,
                        "pec_verificata_at": now_iso,
                        "xml_github_path": xml_path,
                        "fattura_allegata_name": xml_path.split("/")[-1],
                        "ts": payload["ts"],
                    }
                    if not sb_update_fattura(manual_id, update_payload):
                        log.warning(f"  UPDATE fallito per id={manual_id}, skip")
                        break
                else:
                    # INSERT nuova fattura
                    new_id = sb_insert_fattura(payload)
                    if not new_id:
                        log.error(f"  INSERT fallito per {pec_id}")
                        break

                # Estrai prezzi (solo per acquisti)
                try:
                    n_prezzi = estrai_prezzi_da_xml(xml_bytes, fattura, pec_id)
                    if n_prezzi > 0:
                        log.info(f"  [prezzi] +{n_prezzi} righe")
                except Exception as ep:
                    log.warning(f"  [prezzi] errore non bloccante: {ep}")

                # Marca come fatto in memoria (no file da salvare!)
                existing_pec_ids.add(pec_id)
                new_count += 1
                imported_this_email = True
                log.info(f"  ✓ + {fattura['fornitore']} n.{fattura['numero']} EUR {fattura['importo']}")
                break  # una email = una fattura

            if not imported_this_email:
                log.debug(f"  uid={uid_bare}: nessun XML valido (skip silente)")

    finally:
        try:
            imap.logout()
        except:
            pass

    log.info(f"=== Completata: +{new_count} nuove, {skipped_existing} dedup skip, {failed_count} falliti ===")


if __name__ == "__main__":
    def _timeout_handler(signum, frame):
        log.error("=== TIMEOUT GLOBALE 300s — sync interrotta ===")
        raise SystemExit(1)
    try:
        signal.signal(signal.SIGALRM, _timeout_handler)
        signal.alarm(300)
    except (AttributeError, OSError):
        pass
    try:
        sync()
        log.info("=== Sync v15 completata con successo ===")
    except SystemExit:
        log.error("=== Sync interrotta per timeout ===")
        raise
    except Exception as e:
        import traceback
        log.error(f"=== Sync fallita: {e} ===")
        log.error(traceback.format_exc())
        raise
