# Ceraldi PEC Sync — Guida Setup

## Cosa fa
Ogni 24 ore legge la tua PEC Aruba, trova le fatture XML SDI,
le parsea e le salva su Google Drive. L'app Ceraldi Fatture
le importa automaticamente con il bottone "Sincronizza PEC".

---

## Passo 1 — Google Drive: crea la cartella e il Service Account

### 1a. Crea la cartella su Drive
1. Vai su drive.google.com
2. Crea una cartella chiamata `Ceraldi Fatture PEC`
3. Apri la cartella → copia l'ID dall'URL:
   `https://drive.google.com/drive/folders/`**`QUESTO_È_L_ID`**

### 1b. Crea un Service Account Google
1. Vai su https://console.cloud.google.com
2. Crea un nuovo progetto (es. "Ceraldi PEC")
3. Vai su **API e servizi → Libreria** → abilita **Google Drive API**
4. Vai su **API e servizi → Credenziali → Crea credenziali → Account di servizio**
5. Dai un nome (es. "ceraldi-sync"), clicca Crea e continua
6. Salta i permessi opzionali → Fine
7. Nella lista account di servizio, clicca sull'account creato
8. Vai su **Chiavi → Aggiungi chiave → Crea nuova chiave → JSON**
9. Scarica il file JSON (lo useremo dopo)

### 1c. Condividi la cartella con il Service Account
1. Apri il file JSON scaricato, copia il valore di `"client_email"`
   (es. `ceraldi-sync@ceraldi-pec.iam.gserviceaccount.com`)
2. Torna alla cartella Drive → click destro → Condividi
3. Aggiungi quella email con permesso **Editor**

---

## Passo 2 — Deploy su Render

1. Crea un account su https://render.com (gratuito)
2. **New → Background Worker**
3. Collega il tuo repository GitHub (fai upload di questi file)
   oppure usa **Deploy from repo** con questi file
4. Usa `render.yaml` già incluso
5. Nella sezione **Environment Variables** aggiungi:

| Variabile | Valore |
|-----------|--------|
| `PEC_USER` | tua email PEC (es. `nome@pec.it`) |
| `PEC_PASS` | password PEC Aruba |
| `DRIVE_FOLDER_ID` | ID cartella copiato al Passo 1a |
| `GOOGLE_SERVICE_ACCOUNT_JSON` | contenuto INTERO del file JSON (tutto, incluse `{}`) |

6. Clicca **Create Background Worker** → il servizio parte

---

## Passo 3 — Configura l'app Ceraldi Fatture

Nell'app, vai su **Impostazioni** (⚙️) e inserisci:
- **Drive Folder ID**: lo stesso ID della cartella
- L'app ora mostra il bottone **"🔄 Sincronizza PEC"**

---

## Come funziona il sync

```
PEC Aruba (IMAP SSL) 
  ↓ ogni 24h
pec_sync.py (Render)
  ↓ parsea XML SDI
  ↓ carica file su Drive
  ↓ aggiorna ceraldi_fatture_index.json
App Ceraldi Fatture
  ↓ legge ceraldi_fatture_index.json
  ↓ importa fatture nuove nel localStorage
```

---

## Risoluzione problemi

**"Login failed" IMAP**
- Verifica che la PEC sia abilitata all'accesso IMAP (Aruba: Webmail → Impostazioni → IMAP)
- Prova la password accedendo su webmail.pec.aruba.it

**"Service account permission denied"**
- Assicurati di aver condiviso la cartella Drive con l'email del service account

**Lo script non trova fatture**
- Le email PEC con fatture SDI hanno allegati `.xml` o `.xml.p7m`
- Verifica che le fatture arrivino davvero su quella PEC

---

## Free tier Render
Il piano gratuito va benissimo per questo uso:
- 750 ore/mese gratis (un worker usa ~720h/mese)
- Si "addormenta" dopo inattività ma per un worker in loop non succede
