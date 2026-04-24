#!/usr/bin/env python3
"""
PATCH pec_sync.py — Supporto Note di Credito (TD04, TD05, TD08, TD24, TD25)
Applica questa patch al tuo pec_sync.py nel repo ceraldi-pec-sync

USO: python3 pec_sync_nota_credito.py pec_sync.py
"""

import sys, re

NC_CODES = {'TD04', 'TD05', 'TD08', 'TD24', 'TD25'}
NC_NAMES = {
    'TD04': 'Nota di Credito',
    'TD05': 'Nota di Debito',
    'TD08': 'Nota di Credito Semplificata',
    'TD24': 'Fattura Differita con N/C',
    'TD25': 'Fattura con Nota di Credito',
}

# ──────────────────────────────────────────────────────
# Snippet da aggiungere in _parse_xml_fattura (o equivalente)
# Cerca la funzione che estrae i dati dall'XML FatturaPA
# ──────────────────────────────────────────────────────

TIPO_DOC_SNIPPET = '''
    # ── TIPO DOCUMENTO (nota di credito) ──
    NC_CODES = {'TD04', 'TD05', 'TD08', 'TD24', 'TD25'}
    tipo_documento = ''
    for tag in ['TipoDocumento', '{*}TipoDocumento']:
        td_el = root.find('.//' + tag)
        if td_el is None:
            td_el = root.find('.//{http://ivaservizi.agenziaentrate.gov.it/docs/xsd/fatture/v1.2}TipoDocumento')
        if td_el is not None:
            tipo_documento = td_el.text.strip().upper()
            break
    is_nc = tipo_documento in NC_CODES
    tipo_record = 'nota_credito' if is_nc else 'fattura'
'''

def apply_patch(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    original = content
    patches_applied = []

    # ── PATCH 1: Aggiunge tipo_documento nell'estrazione XML ──
    # Cerca blocco che estrae 'importo' o 'numero' dall'XML
    # e inserisce il rilevamento tipo dopo
    
    # Pattern: cerca riga dove si crea il dict result/record con fornitore
    patterns_insert = [
        # Cerca dopo "numero_fattura" o "numero" nell'estrazione
        (r"(numero\s*=\s*[^\n]+\n)", r"\1" + "    tipo_documento = _get_tipo_documento(root)\n    is_nc = tipo_documento in {'TD04','TD05','TD08','TD24','TD25'}\n"),
        # Cerca dopo assegnazione importo totale
        (r"(importo_totale\s*=\s*[^\n]+\n)", r"\1    tipo_documento = _get_tipo_documento(root)\n    is_nc = tipo_documento in {'TD04','TD05','TD08','TD24','TD25'}\n"),
    ]
    
    # ── PATCH 2: Aggiunge helper _get_tipo_documento ──
    helper_fn = '''
def _get_tipo_documento(root):
    """Estrae TipoDocumento da XML FatturaPA."""
    namespaces = [
        '',
        '{http://ivaservizi.agenziaentrate.gov.it/docs/xsd/fatture/v1.2}',
        '{http://ivaservizi.agenziaentrate.gov.it/docs/xsd/fatture/v1.2.1}',
    ]
    for ns in namespaces:
        el = root.find('.//' + ns + 'TipoDocumento')
        if el is not None and el.text:
            return el.text.strip().upper()
    return 'TD01'  # default: fattura ordinaria

'''

    # Inserisci l'helper prima della funzione principale di parsing
    if '_get_tipo_documento' not in content:
        # Inserisci prima della prima funzione def _parse o def parse
        m = re.search(r'(\ndef (?:_parse|parse|extract))', content)
        if m:
            content = content[:m.start()] + '\n' + helper_fn + content[m.start():]
            patches_applied.append("Aggiunto helper _get_tipo_documento()")

    # ── PATCH 3: Nel payload Supabase, aggiungi tipo ──
    # Cerca dizionario con 'fornitore', 'numero', 'importo' e aggiungi 'tipo'
    
    # Pattern per payload/record dict
    payload_patterns = [
        # 'tipo': 'fattura'  → rimpiazza con tipo_record
        (r"'tipo'\s*:\s*'fattura'", "'tipo': tipo_record"),
        (r'"tipo"\s*:\s*"fattura"', '"tipo": tipo_record'),
        # Se non c'è 'tipo' nel payload, aggiungilo dopo 'fornitore'
        (r"('fornitore'\s*:\s*[^,\n]+,)", r"\1\n            'tipo': tipo_record,"),
    ]
    
    for pattern, replacement in payload_patterns:
        new_content = re.sub(pattern, replacement, content)
        if new_content != content:
            content = new_content
            patches_applied.append(f"Sostituito pattern: {pattern[:50]}...")
            break

    # ── PATCH 4: Aggiungi variabile tipo_record se non c'è ──
    if 'tipo_record' not in content and '_get_tipo_documento' in content:
        # Aggiungi dopo ogni chiamata a _get_tipo_documento o prima del payload
        content = re.sub(
            r"(_get_tipo_documento\([^)]+\))",
            r"_get_tipo_documento(\1.replace('_get_tipo_documento(','').replace(')',''))",
            content
        )
        # Semplifica: aggiungi blocco completo
        insert = '''
    # ── TIPO DOCUMENTO ──
    tipo_documento = _get_tipo_documento(root)
    NC_CODES_SET = {'TD04', 'TD05', 'TD08', 'TD24', 'TD25'}
    tipo_record = 'nota_credito' if tipo_documento in NC_CODES_SET else 'fattura'
    if tipo_record == 'nota_credito':
        logger.info(f"  📋 NOTA DI CREDITO ({tipo_documento}) rilevata")
'''
        # Inserisci prima del return/payload
        m = re.search(r'(\n    return\s*\{)', content)
        if m:
            content = content[:m.start()] + insert + content[m.start():]
            patches_applied.append("Aggiunto blocco tipo_documento + tipo_record")

    if content == original:
        print("⚠️  Nessuna patch applicata automaticamente.")
        print("   Applica manualmente le seguenti modifiche al tuo pec_sync.py:")
        print()
        print("1. Aggiungi questa funzione helper:")
        print(helper_fn)
        print("2. Nel blocco di estrazione XML, aggiungi:")
        print("   tipo_documento = _get_tipo_documento(root)")
        print("   tipo_record = 'nota_credito' if tipo_documento in {'TD04','TD05','TD08','TD24','TD25'} else 'fattura'")
        print("3. Nel payload Supabase, aggiungi:")
        print("   'tipo': tipo_record,")
        return False

    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print(f"✅ Patch applicate a {filepath}:")
    for p in patches_applied:
        print(f"   - {p}")
    return True


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Uso: python3 pec_sync_nota_credito.py pec_sync.py")
        print()
        print("Questo script patcha pec_sync.py per supportare le Note di Credito.")
        print("Tipi supportati: TD04 (N/C), TD05 (N/D), TD08 (N/C semplificata),")
        print("                 TD24 (Fattura differita + N/C), TD25 (Fattura + N/C)")
        sys.exit(1)
    apply_patch(sys.argv[1])
