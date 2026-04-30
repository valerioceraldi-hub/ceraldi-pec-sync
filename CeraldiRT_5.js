// ── CERALDI RT IMPORT v5 ──
const RT_URL   = 'http://192.168.1.19/www/dati-rt/';
const SB_URL   = 'https://qaqqptpprmfjlolordaq.supabase.co';
const SB_KEY   = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InFhcXFwdHBwcm1mamxvbG9yZGFxIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzU4NDQ3MDgsImV4cCI6MjA5MTQyMDcwOH0.kTnxsNY3tua_ya4LCB8-vkVdQ1QBPGtLL7Gfg121d1o';
const SB_TABLE = 'chiusure_giornaliere';

// ── UI ──
let alert = new Alert();
alert.title = '📥 Ceraldi RT Import';
alert.message = 'Scegli modalità';
alert.addAction('▶ Avvia completo');
alert.addAction('🔍 Test 1 data (con debug)');
alert.addCancelAction('Annulla');
let risposta = await alert.present();
if (risposta === -1) return;
let soloTest = (risposta === 1);

// ── STEP 1: Indice cartelle ──
let indiceReq = new Request(RT_URL);
indiceReq.timeoutInterval = 10;
let indiceHtml;
try {
  indiceHtml = await indiceReq.loadString();
} catch(e) {
  await notifica('❌ Stampante non raggiungibile', e.message);
  return;
}

let cartelleMatch = indiceHtml.match(/href="(2\d{7})\/"/g) || [];
let cartelle = cartelleMatch.map(m => m.replace('href="','').replace('/"',''));
log('Cartelle trovate: ' + cartelle.length);
if (!cartelle.length) { await notifica('⚠️', 'Nessuna cartella trovata'); return; }

// ── STEP 2: Chiusure già su Supabase ──
let esistentiReq = new Request(SB_URL + '/rest/v1/' + SB_TABLE + '?select=data');
esistentiReq.headers = { 'apikey': SB_KEY, 'Authorization': 'Bearer ' + SB_KEY };
let esistentiJson;
try { esistentiJson = await esistentiReq.loadJSON(); }
catch(e) { await notifica('❌ Errore lettura Supabase', e.message); return; }
let dateEsistenti = new Set((esistentiJson||[]).map(r => r.data));
log('Date già su Supabase: ' + dateEsistenti.size);

// ── STEP 3: Processa cartelle ──
let nuove = 0, saltate = 0, errori = 0, erroriDet = [];

for (let cartella of cartelle) {
  let data = cartella.slice(0,4)+'-'+cartella.slice(4,6)+'-'+cartella.slice(6,8);
  if (dateEsistenti.has(data)) { saltate++; continue; }

  try {
    // Scarica indice cartella
    let dirReq = new Request(RT_URL + cartella + '/');
    dirReq.timeoutInterval = 8;
    let dirHtml = await dirReq.loadString();
    let zMatch = dirHtml.match(/href="([^"]+ZREPORT\.txt)"/i);
    if (!zMatch) { saltate++; continue; }

    // Scarica ZREPORT.txt
    let zReq = new Request(RT_URL + cartella + '/' + zMatch[1]);
    zReq.timeoutInterval = 8;
    let zTxt = await zReq.loadString();

    let chiusura = parseZReport(zTxt, data);
    if (!chiusura) { saltate++; continue; }

    if (soloTest) {
      // Mostra il JSON che verrà mandato a Supabase
      let dbg = new Alert();
      dbg.title = '🔍 ' + data;
      dbg.message = 'JSON che mando a Supabase:\n\n' + JSON.stringify(chiusura, null, 2);
      dbg.addAction('Salva ora');
      dbg.addCancelAction('Annulla');
      let conf = await dbg.present();
      if (conf === -1) { log('Test annullato'); return; }
    }

    // ── SALVA SU SUPABASE ──
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
    log('Supabase risposta [' + data + ']: ' + saveResp.slice(0, 300));

    // Controlla errore nella risposta
    if (saveResp.includes('"code"') || saveResp.includes('"error"')) {
      errori++;
      erroriDet.push(data + ': ' + saveResp.slice(0, 150));
      if (soloTest) {
        await notifica('❌ Errore Supabase', saveResp.slice(0, 300));
        return;
      }
      continue;
    }

    nuove++;
    if (soloTest) {
      await notifica('✅ Salvato!', 'Risposta Supabase:\n' + saveResp.slice(0, 300));
      return;
    }

  } catch(e) {
    errori++;
    erroriDet.push(data + ': ' + e.message);
    if (soloTest) { await notifica('❌ Eccezione', e.message); return; }
  }

  if ((nuove + errori) % 5 === 0) await sleep(300);
}

// ── RISULTATO ──
let msg = '✅ Nuove: '+nuove+'\n⏭ Saltate: '+saltate+'\n❌ Errori: '+errori;
if (erroriDet.length) msg += '\n\n' + erroriDet.slice(0,5).join('\n');
let fine = new Alert();
fine.title = nuove > 0 ? '✅ Import completato' : '📋 Nessuna novità';
fine.message = msg;
fine.addAction('OK');
await fine.present();

// ── PARSER ──
function parseZReport(txt, data) {
  if (!txt || !txt.trim()) return null;

  function italFloat(s) {
    if (!s) return 0;
    return parseFloat(s.replace(/\./g,'').replace(',','.')) || 0;
  }
  function cerca(pattern) {
    let m = txt.match(new RegExp(pattern + '\\s+([\\d.,]+)\\s*$', 'im'));
    return m ? italFloat(m[1]) : 0;
  }
  function cercaIva(aliquota) {
    let m = txt.match(new RegExp('ALIQUOTA IVA\\s+'+aliquota+'%[\\s\\S]*?AMMONTARE\\s+([\\d.,]+)', 'im'));
    return m ? italFloat(m[1]) : 0;
  }

  let totale      = cerca('TOTALE GIORNO VENDITE');
  let contanti    = cerca('PAGATO CONTANTI');
  let elettronico = cerca('PAGATO ELETTRONICO');
  let iva10 = cercaIva('10\\.00');
  let iva22 = cercaIva('22\\.00');
  let iva4  = cercaIva('4\\.00');

  if (totale === 0 && (contanti > 0 || elettronico > 0)) totale = contanti + elettronico;
  if (totale === 0 && contanti === 0 && elettronico === 0) return null;

  // id numerico: timestamp basato sulla data (evita conflitti con tipo colonna INTEGER)
  let dateParts = data.split('-'); // ['2025','11','03']
  let idNum = parseInt(dateParts[0]) * 10000 + parseInt(dateParts[1]) * 100 + parseInt(dateParts[2]);

  let row = {
    id: idNum,
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
  a.title = titolo; a.message = msg; a.addAction('OK');
  await a.present();
}
