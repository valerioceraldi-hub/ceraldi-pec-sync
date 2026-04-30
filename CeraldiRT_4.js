// ── CERALDI RT IMPORT v4 ──
// Script Scriptable per scaricare le chiusure dalla stampante fiscale
// e salvarle su Supabase (tabella chiusure_giornaliere)

const RT_URL   = 'http://192.168.1.19/www/dati-rt/';
const SB_URL   = 'https://qaqqptpprmfjlolordaq.supabase.co';
const SB_KEY   = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InFhcXFwdHBwcm1mamxvbG9yZGFxIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzU4NDQ3MDgsImV4cCI6MjA5MTQyMDcwOH0.kTnxsNY3tua_ya4LCB8-vkVdQ1QBPGtLL7Gfg121d1o';
const SB_TABLE = 'chiusure_giornaliere';

// ── UI ──
let alert = new Alert();
alert.title = '📥 Ceraldi RT Import';
alert.message = 'Scarica chiusure dalla stampante fiscale e aggiorna Supabase';
alert.addAction('▶ Avvia completo');
alert.addAction('🔍 Test (solo prima data)');
alert.addCancelAction('Annulla');
let risposta = await alert.present();
if (risposta === -1) return;
let soloTest = (risposta === 1);

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

// ── STEP 2: Recupera chiusure già presenti su Supabase ──
log('Recupero chiusure esistenti da Supabase...');
let esistentiReq = new Request(SB_URL + '/rest/v1/' + SB_TABLE + '?select=data');
esistentiReq.headers = { 'apikey': SB_KEY, 'Authorization': 'Bearer ' + SB_KEY };
let esistentiJson;
try {
  esistentiJson = await esistentiReq.loadJSON();
} catch(e) {
  await notifica('❌ Errore Supabase', 'Impossibile leggere le chiusure esistenti:\n' + e.message);
  return;
}
let dateEsistenti = new Set((esistentiJson || []).map(r => r.data));
log('Chiusure già presenti: ' + dateEsistenti.size);

// ── STEP 3: Processa solo le cartelle mancanti ──
let nuove = 0, saltate = 0, errori = 0;
let erroriDettaglio = [];

for (let cartella of cartelle) {
  let data = cartella.slice(0,4) + '-' + cartella.slice(4,6) + '-' + cartella.slice(6,8);
  
  if (dateEsistenti.has(data)) { saltate++; continue; }

  try {
    let dirReq = new Request(RT_URL + cartella + '/');
    dirReq.timeoutInterval = 8;
    let dirHtml = await dirReq.loadString();
    
    let zMatch = dirHtml.match(/href="([^"]+ZREPORT\.txt)"/i);
    if (!zMatch) { saltate++; continue; }
    
    let zFile = zMatch[1];
    let zUrl = RT_URL + cartella + '/' + zFile;
    
    let zReq = new Request(zUrl);
    zReq.timeoutInterval = 8;
    let zTxt = await zReq.loadString();
    
    let chiusura = parseZReport(zTxt, data);
    if (!chiusura) { saltate++; continue; }
    
    // In modalità test: mostra valori e chiedi conferma
    if (soloTest) {
      let dbg = new Alert();
      dbg.title = '🔍 Parser: ' + data;
      dbg.message = 'Totale: ' + chiusura.totale_corrispettivi + ' euro' +
        '\nContanti: ' + chiusura.cassa + ' euro' +
        '\nPOS: ' + chiusura.pos + ' euro' +
        '\n\nValori corretti?';
      dbg.addAction('Sì — salva questa');
      dbg.addDestructiveAction('No — annulla tutto');
      let conf = await dbg.present();
      if (conf === 1) { errori++; erroriDettaglio.push(data + ': annullato'); continue; }
    }

    // ── SALVA SU SUPABASE ──
    // IMPORTANTE: usa return=representation per vedere la risposta e catturare errori
    let saveReq = new Request(SB_URL + '/rest/v1/' + SB_TABLE);
    saveReq.method = 'POST';
    saveReq.headers = {
      'apikey': SB_KEY,
      'Authorization': 'Bearer ' + SB_KEY,
      'Content-Type': 'application/json',
      'Prefer': 'resolution=merge-duplicates,return=representation'
    };
    saveReq.body = JSON.stringify(chiusura);
    
    let saveResp = await saveReq.loadString();
    log('Risposta Supabase per ' + data + ': ' + saveResp.slice(0, 200));
    
    // Controlla se Supabase ha restituito errore (es. {"code":"...","message":"..."})
    if (saveResp.includes('"code"') && saveResp.includes('"message"')) {
      try {
        let err = JSON.parse(saveResp);
        if (err.message) {
          errori++;
          erroriDettaglio.push(data + ': ' + err.message.slice(0, 80));
          continue;
        }
      } catch(pe) {}
    }
    
    nuove++;
    
  } catch(e) {
    errori++;
    erroriDettaglio.push(data + ': ' + e.message);
  }
  
  if ((nuove + errori) % 5 === 0) await sleep(300);
}

// ── RISULTATO ──
let msg = '✅ Nuove: ' + nuove + '\n⏭ Saltate: ' + saltate + '\n❌ Errori: ' + errori;
if (erroriDettaglio.length > 0) msg += '\n\nDettaglio:\n' + erroriDettaglio.slice(0, 5).join('\n');

let fine = new Alert();
fine.title = nuove > 0 ? '✅ Import completato' : '📋 Nessuna novità';
fine.message = msg;
fine.addAction('OK');
await fine.present();

// ── PARSER ZREPORT.TXT ──
function parseZReport(txt, data) {
  if (!txt || txt.trim().length === 0) return null;
  
  function italFloat(s) {
    if (!s) return 0;
    return parseFloat(s.replace(/\./g, '').replace(',', '.')) || 0;
  }
  
  function cerca(pattern) {
    let r = new RegExp(pattern + '\\s+([\\d.,]+)\\s*$', 'im');
    let m = txt.match(r);
    return m ? italFloat(m[1]) : 0;
  }
  
  let totale      = cerca('TOTALE GIORNO VENDITE');
  let contanti    = cerca('PAGATO CONTANTI');
  let elettronico = cerca('PAGATO ELETTRONICO');
  
  function cercaIva(aliquota) {
    let pat = new RegExp('ALIQUOTA IVA\\s+' + aliquota + '%[\\s\\S]*?AMMONTARE\\s+([\\d.,]+)', 'im');
    let m = txt.match(pat);
    return m ? italFloat(m[1]) : 0;
  }
  
  let iva10 = cercaIva('10\\.00');
  let iva22 = cercaIva('22\\.00');
  let iva4  = cercaIva('4\\.00');
  
  if (totale === 0 && (contanti > 0 || elettronico > 0)) {
    totale = contanti + elettronico;
  }
  
  if (totale === 0 && contanti === 0 && elettronico === 0) return null;
  
  // Genera id deterministico dalla data (stesso formato usato dall'app)
  // "epson_" + data senza trattini → es. epson_20251103
  // Questo evita duplicati anche se lo script gira più volte
  let id = 'epson_' + data.replace(/-/g, '');
  
  let row = {
    id: id,
    data: data,
    cassa: contanti,
    pos: elettronico,
    totale_corrispettivi: totale,
    incassato: Math.round((contanti + elettronico) * 100) / 100,
    differenza: Math.round((contanti + elettronico - totale) * 100) / 100,
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
