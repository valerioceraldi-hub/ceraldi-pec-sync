// ── CERALDI RT IMPORT ──
// Script Scriptable per scaricare le chiusure dalla stampante fiscale
// e salvarle su Supabase (tabella chiusure_giornaliere)
//
// INSTALLAZIONE:
// 1. Installa Scriptable dall'App Store
// 2. Crea nuovo script, incolla tutto questo codice
// 3. Premi Run quando sei al bar connesso al WiFi

const RT_URL   = 'http://192.168.1.19/www/dati-rt/';
const SB_URL   = 'https://qaqqptpprmfjlolordaq.supabase.co';
const SB_KEY   = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InFhcXFwdHBwcm1mamxvbG9yZGFxIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzU4NDQ3MDgsImV4cCI6MjA5MTQyMDcwOH0.kTnxsNY3tua_ya4LCB8-vkVdQ1QBPGtLL7Gfg121d1o';
const SB_TABLE = 'chiusure_giornaliere';

// ── UI ──
let alert = new Alert();
alert.title = '📥 Ceraldi RT Import';
alert.message = 'Scarica chiusure dalla stampante fiscale e aggiorna Supabase';
alert.addAction('▶ Avvia');
alert.addCancelAction('Annulla');
let risposta = await alert.present();
if (risposta === -1) return;

// ── STEP 1: Scarica indice cartelle ──
log('Connessione a ' + RT_URL);
let indiceReq = new Request(RT_URL);
indiceReq.timeoutInterval = 10;
let indiceHtml;
try {
  indiceHtml = await indiceReq.loadString();
} catch(e) {
  await notifica('❌ Impossibile connettersi', 'Assicurati di essere sul WiFi del bar.\n' + e.message);
  return;
}

// Estrai le cartelle dall'index Apache (es. 20251103/)
let cartelleMatch = indiceHtml.match(/href="(2\d{7})\/"/g) || [];
let cartelle = cartelleMatch.map(m => m.replace('href="', '').replace('/"', ''));
log('Trovate ' + cartelle.length + ' cartelle');

if (cartelle.length === 0) {
  await notifica('⚠️ Nessuna cartella trovata', 'La stampante non ha dati disponibili.');
  return;
}

// ── STEP 2: Recupera chiusure già presenti su Supabase (per evitare duplicati) ──
log('Recupero chiusure esistenti da Supabase...');
let esistentiReq = new Request(SB_URL + '/rest/v1/' + SB_TABLE + '?select=data');
esistentiReq.headers = { 'apikey': SB_KEY, 'Authorization': 'Bearer ' + SB_KEY };
let esistentiJson = await esistentiReq.loadJSON();
let dateEsistenti = new Set((esistentiJson || []).map(r => r.data));
log('Chiusure già presenti: ' + dateEsistenti.size);

// ── STEP 3: Processa solo le cartelle mancanti ──
let nuove = 0, saltate = 0, errori = 0;
let erroriDettaglio = [];

for (let cartella of cartelle) {
  // Converti nome cartella in data: 20251103 → 2025-11-03
  let data = cartella.slice(0,4) + '-' + cartella.slice(4,6) + '-' + cartella.slice(6,8);
  
  // Salta se già presente su Supabase
  if (dateEsistenti.has(data)) {
    saltate++;
    continue;
  }

  // Scarica indice della cartella per trovare il file ZREPORT.txt
  try {
    let dirReq = new Request(RT_URL + cartella + '/');
    dirReq.timeoutInterval = 8;
    let dirHtml = await dirReq.loadString();
    
    // Trova il file ZREPORT.txt
    let zMatch = dirHtml.match(/href="([^"]+ZREPORT\.txt)"/i);
    if (!zMatch) { saltate++; continue; }
    
    let zFile = zMatch[1];
    let zUrl = RT_URL + cartella + '/' + zFile;
    
    // Scarica il file ZREPORT.txt
    let zReq = new Request(zUrl);
    zReq.timeoutInterval = 8;
    let zTxt = await zReq.loadString();
    
    // Parsa il file
    let chiusura = parseZReport(zTxt, data);
    if (!chiusura) { saltate++; continue; }
    
    // Debug: mostra prima chiusura parsata
    if (!primaChiusura) {
      primaChiusura = chiusura;
      let dbg = new Alert();
      dbg.title = '🔍 Test parser: ' + data;
      dbg.message = 'Totale: €' + chiusura.totale_corrispettivi + '\nContanti: €' + chiusura.cassa + '\nPOS: €' + chiusura.pos + '\n\nCorretto?';
      dbg.addAction('Sì, procedi');
      dbg.addDestructiveAction('No, annulla');
      let r = await dbg.present();
      if (r === 1) return;
    }

    // Salva su Supabase
    let saveReq = new Request(SB_URL + '/rest/v1/' + SB_TABLE);
    saveReq.method = 'POST';
    saveReq.headers = {
      'apikey': SB_KEY,
      'Authorization': 'Bearer ' + SB_KEY,
      'Content-Type': 'application/json',
      'Prefer': 'resolution=merge-duplicates,return=minimal'
    };
    saveReq.body = JSON.stringify(chiusura);
    let saveResp = await saveReq.loadString();
    nuove++;
    
  } catch(e) {
    errori++;
    erroriDettaglio.push(data + ': ' + e.message);
  }
  
  // Pausa ogni 5 per non sovraccaricare
  if ((nuove + errori) % 5 === 0) await sleep(300);
}

// ── RISULTATO ──
let msg = '✅ Nuove: ' + nuove + '\n⏭ Saltate: ' + saltate + '\n❌ Errori: ' + errori;
if (erroriDettaglio.length > 0) msg += '\n\n' + erroriDettaglio.slice(0,3).join('\n');

let fine = new Alert();
fine.title = nuove > 0 ? '✅ Import completato' : '📋 Nessuna novità';
fine.message = msg;
fine.addAction('OK');
await fine.present();

// ── PARSER ZREPORT.TXT ──
// Formato Ceraldi: campo + spazi + valore numerico italiano (es. "2.116,82")
function parseZReport(txt, data) {
  if (!txt || txt.trim().length === 0) return null;
  
  // Converti numero italiano "2.116,82" → float 2116.82
  function italFloat(s) {
    if (!s) return 0;
    return parseFloat(s.replace(/\./g, '').replace(',', '.')) || 0;
  }
  
  // Cerca riga che contiene pattern e restituisce il numero a destra
  function cerca(pattern) {
    let r = new RegExp(pattern + '\\s+([\\d.,]+)\\s*$', 'im');
    let m = txt.match(r);
    return m ? italFloat(m[1]) : 0;
  }
  
  let totale    = cerca('TOTALE GIORNO VENDITE');
  let contanti  = cerca('PAGATO CONTANTI');
  let elettronico = cerca('PAGATO ELETTRONICO');
  
  // Estrai aliquote IVA — cerca sezione "ALIQUOTA IVA XX.XX%" poi AMMONTARE
  function cercaIva(aliquota) {
    let pat = new RegExp('ALIQUOTA IVA\\s+' + aliquota + '%[\\s\\S]*?AMMONTARE\\s+([\\d.,]+)', 'im');
    let m = txt.match(pat);
    return m ? italFloat(m[1]) : 0;
  }
  
  let iva10 = cercaIva('10\\.00');
  let iva22 = cercaIva('22\\.00');
  let iva4  = cercaIva('4\\.00');
  
  // Se totale è 0 ma abbiamo contanti/elettronico, calcoliamo
  if (totale === 0 && (contanti > 0 || elettronico > 0)) {
    totale = contanti + elettronico;
  }
  
  if (totale === 0 && contanti === 0 && elettronico === 0) return null;
  
  let row = {
    data: data,
    cassa: contanti,
    pos: elettronico,
    totale_corrispettivi: totale,
    note: 'Importato da RT ' + data,
    updated_at: new Date().toISOString()
  };
  
  if (iva10 > 0) row.iva10 = iva10;
  if (iva22 > 0) row.iva22 = iva22;
  if (iva4  > 0) row.iva4  = iva4;
  
  return row;
}

function sleep(ms) {
  return new Promise(r => Timer.schedule(ms, false, r));
}

async function notifica(titolo, msg) {
  let a = new Alert();
  a.title = titolo;
  a.message = msg;
  a.addAction('OK');
  await a.present();
}
