/**
 * server/server.js
 * نسخة معدّلة: إصلاحات login، charge, genericBotReplyHandler (charges)، poll loop، retry initSheets.
 * مضافة ميزة: loginNumber مخزّن في العمود G بورقة Profiles (A..G)
 */

const express = require('express');
const cors = require('cors');
const fetch = require('node-fetch');
const fs = require('fs');
const path = require('path');
const multer = require('multer');
const { google } = require('googleapis');

let sheetsClient = null;
async function initSheets() {
  try {
    let credsJson = process.env.GOOGLE_SA_KEY_JSON || null;
    if (!credsJson && process.env.GOOGLE_SA_CRED_PATH && fs.existsSync(process.env.GOOGLE_SA_CRED_PATH)) {
      credsJson = fs.readFileSync(process.env.GOOGLE_SA_CRED_PATH, 'utf8');
    }
    if (!credsJson) {
      console.warn('Google Sheets credentials not provided (GOOGLE_SA_KEY_JSON or GOOGLE_SA_CRED_PATH). Sheets disabled.');
      return;
    }
    const creds = typeof credsJson === 'string' ? JSON.parse(credsJson) : credsJson;
    const jwt = new google.auth.JWT(
      creds.client_email,
      null,
      creds.private_key,
      ['https://www.googleapis.com/auth/spreadsheets']
    );
    await jwt.authorize();
    sheetsClient = google.sheets({ version: 'v4', auth: jwt });
    console.log('Google Sheets initialized');
  } catch (e) {
    console.warn('initSheets error', e);
    sheetsClient = null;
  }
}
initSheets();

// retry initSheets every 60s if not ready
setInterval(() => {
  if (!sheetsClient) {
    console.log('Attempting to re-init Google Sheets client...');
    initSheets().catch(()=>{});
  }
}, 60_000);

const SPREADSHEET_ID = process.env.SHEET_ID || null;

/**
 * اقرأ صف البروفايل من شيت Profiles (A..G) وترجعه كـ object أو null
 */
async function getProfileFromSheet(personal) {
  if (!sheetsClient || !SPREADSHEET_ID) return null;
  try {
    const resp = await sheetsClient.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Profiles!A2:G10000',
    });
    const rows = (resp.data && resp.data.values) || [];
    for (let i = 0; i < rows.length; i++) {
      const r = rows[i];
      if (String(r[0] || '') === String(personal)) {
        return {
          rowIndex: i + 2,
          personalNumber: r[0] || '',
          name: r[1] || '',
          email: r[2] || '',
          password: r[3] || '',
          phone: r[4] || '',
          balance: Number(r[5] || 0),
          loginNumber: (r[6] != null && String(r[6]).trim() !== '') ? Number(r[6]) : null
        };
      }
    }
    return null;
  } catch (e) {
    console.warn('getProfileFromSheet error', e);
    return null;
  }
}

/**
 * اضف او حدّث صف في الشيت (upsert) — يكتب الآن العمود G = loginNumber
 */
async function upsertProfileRow(profile) {
  if (!sheetsClient || !SPREADSHEET_ID) return false;
  try {
    const existing = await getProfileFromSheet(profile.personalNumber);
    const values = [
      String(profile.personalNumber || ''),
      profile.name || '',
      profile.email || '',
      profile.password || '',
      profile.phone || '',
      String(profile.balance == null ? 0 : profile.balance),
      profile.loginNumber != null ? String(profile.loginNumber) : ''
    ];
    if (existing && existing.rowIndex) {
      const range = `Profiles!A${existing.rowIndex}:G${existing.rowIndex}`;
      await sheetsClient.spreadsheets.values.update({
        spreadsheetId: SPREADSHEET_ID,
        range,
        valueInputOption: 'RAW',
        requestBody: { values: [values] }
      });
    } else {
      await sheetsClient.spreadsheets.values.append({
        spreadsheetId: SPREADSHEET_ID,
        range: 'Profiles!A2:G2',
        valueInputOption: 'RAW',
        insertDataOption: 'INSERT_ROWS',
        requestBody: { values: [values] }
      });
    }
    return true;
  } catch (e) {
    console.warn('upsertProfileRow error', e);
    return false;
  }
}

/**
 * حدّث الرصيد فقط في الشيت
 */
async function updateBalanceInSheet(personal, newBalance) {
  if (!sheetsClient || !SPREADSHEET_ID) return false;
  try {
    const existing = await getProfileFromSheet(personal);
    if (existing && existing.rowIndex) {
      const range = `Profiles!F${existing.rowIndex}`;
      await sheetsClient.spreadsheets.values.update({
        spreadsheetId: SPREADSHEET_ID,
        range,
        valueInputOption: 'RAW',
        requestBody: { values: [[ String(newBalance) ]] }
      });
      return true;
    } else {
      await upsertProfileRow({ personalNumber: personal, name:'', email:'', password:'', phone:'', balance: newBalance });
      return true;
    }
  } catch (e) {
    console.warn('updateBalanceInSheet error', e);
    return false;
  }
}

/**
 * يعين رقم دخول متسلسل في العمود G داخل Profiles
 * - إن وُجد رقم مسبقًا في الصف يرجع الرقم
 * - إذا لم يوجد، يحسب next = count(non-empty G) + 1، ثم يكتب الرقم في صف المستخدم (أو يضيف صف جديد)
 * - يرجع الرقم (Number) أو null لو فشل
 */
async function assignLoginNumberInProfilesSheet(personal) {
  if (!sheetsClient || !SPREADSHEET_ID) return null;
  try {
    const existing = await getProfileFromSheet(personal);
    if (existing && existing.loginNumber) return Number(existing.loginNumber);

    // جلب كل قيم العمود G
    const resp = await sheetsClient.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Profiles!G2:G10000',
    });
    const rows = (resp.data && resp.data.values) || [];

    // حساب الخانات المملوءة في G
    let filled = 0;
    for (let i = 0; i < rows.length; i++) {
      if (rows[i] && String(rows[i][0] || '').trim() !== '') filled++;
    }
    const nextNumber = filled + 1;

    if (existing && existing.rowIndex) {
      // نكتب الرقم في عمود G لنفس الصف
      const range = `Profiles!G${existing.rowIndex}`;
      await sheetsClient.spreadsheets.values.update({
        spreadsheetId: SPREADSHEET_ID,
        range,
        valueInputOption: 'RAW',
        requestBody: { values: [[ String(nextNumber) ]] }
      });
      return nextNumber;
    } else {
      // لم نجد صفًا مسبقًا — نضيف صفًا جديدًا (A..G) مع loginNumber
      await sheetsClient.spreadsheets.values.append({
        spreadsheetId: SPREADSHEET_ID,
        range: 'Profiles!A2:G2',
        valueInputOption: 'RAW',
        insertDataOption: 'INSERT_ROWS',
        requestBody: { values: [[ String(personal), '', '', '', '', '0', String(nextNumber) ]] }
      });
      return nextNumber;
    }
  } catch (e) {
    console.warn('assignLoginNumberInProfilesSheet error', e);
    return null;
  }
}

/**
 * fallback محلي: لو Sheets غير متوفرة، نقوم بتعيين رقم دخول محلي
 * - يتحقق من DB.profiles إذا في رقم لهذا الشخص يرجعه
 * - وإلا يحسب max(loginNumber) الموجود ويعطي التالي
 */
function assignLoginNumberLocal(personal) {
  let prof = findProfileByPersonal(personal);
  if (!prof) prof = ensureProfile(personal);

  if (prof.loginNumber) return Number(prof.loginNumber);

  let maxNum = 0;
  for (const p of DB.profiles) {
    const n = Number(p.loginNumber || 0);
    if (!isNaN(n) && n > maxNum) maxNum = n;
  }
  const next = maxNum + 1 || 1;
  prof.loginNumber = next;
  saveData(DB);
  return next;
}

const app = express();
const PORT = process.env.PORT || 3000;
app.use(cors({ origin: "*" }));

const CFG = {
  BOT_ORDER_TOKEN: process.env.BOT_ORDER_TOKEN || "",
  BOT_ORDER_CHAT: process.env.BOT_ORDER_CHAT || "",

  BOT_BALANCE_TOKEN: process.env.BOT_BALANCE_TOKEN || "",
  BOT_BALANCE_CHAT: process.env.BOT_BALANCE_CHAT || "",

  BOT_ADMIN_CMD_TOKEN: process.env.BOT_ADMIN_CMD_TOKEN || "",
  BOT_ADMIN_CMD_CHAT: process.env.BOT_ADMIN_CMD_CHAT || "",

  BOT_LOGIN_REPORT_TOKEN: process.env.BOT_LOGIN_REPORT_TOKEN || "",
  BOT_LOGIN_REPORT_CHAT: process.env.BOT_LOGIN_REPORT_CHAT || "",

  BOT_HELP_TOKEN: process.env.BOT_HELP_TOKEN || "",
  BOT_HELP_CHAT: process.env.BOT_HELP_CHAT || "",

  BOT_OFFERS_TOKEN: process.env.BOT_OFFERS_TOKEN || "",
  BOT_OFFERS_CHAT: process.env.BOT_OFFERS_CHAT || "",

  BOT_NOTIFY_TOKEN: process.env.BOT_NOTIFY_TOKEN || "",
  BOT_NOTIFY_CHAT: process.env.BOT_NOTIFY_CHAT || "",

  IMGBB_KEY: process.env.IMGBB_KEY || ""
};

const DATA_FILE = path.join(__dirname, 'data.json');

function loadData(){
  try{
    if(!fs.existsSync(DATA_FILE)){
      const init = {
        profiles: [],
        orders: [],
        charges: [],
        offers: [],
        notifications: [],
        profileEditRequests: {},
        blocked: [],
        tgOffsets: {}
      };
      fs.writeFileSync(DATA_FILE, JSON.stringify(init, null, 2));
      return init;
    }
    const raw = fs.readFileSync(DATA_FILE,'utf8');
    return JSON.parse(raw || '{}');
  }catch(e){
    console.error('loadData error', e);
    return { profiles:[], orders:[], charges:[], offers:[], notifications:[], profileEditRequests:{}, blocked:[], tgOffsets:{} };
  }
}
function saveData(d){ try{ fs.writeFileSync(DATA_FILE, JSON.stringify(d, null, 2)); }catch(e){ console.error('saveData error', e); } }
let DB = loadData();

function findProfileByPersonal(n){
  return DB.profiles.find(p => String(p.personalNumber) === String(n)) || null;
}
function ensureProfile(personal){
  let p = findProfileByPersonal(personal);
  if(!p){
    p = { personalNumber: String(personal), name: 'ضيف', email:'', phone:'', password:'', balance: 0, canEdit:false };
    DB.profiles.push(p); saveData(DB);
  } else {
    if(typeof p.balance === 'undefined') p.balance = 0;
  }
  return p;
}

app.use(express.json({limit:'10mb'}));
app.use(express.urlencoded({ extended:true, limit:'10mb'}));

const PUBLIC_DIR = path.join(__dirname, 'public');
if(!fs.existsSync(PUBLIC_DIR)) fs.mkdirSync(PUBLIC_DIR, { recursive: true });
app.use('/', express.static(PUBLIC_DIR));

const UPLOADS_DIR = path.join(PUBLIC_DIR, 'uploads');
if(!fs.existsSync(UPLOADS_DIR)) fs.mkdirSync(UPLOADS_DIR, { recursive: true });

const memoryStorage = multer.memoryStorage();
const uploadMemory = multer({ storage: memoryStorage });

async function sendTelegramWithTimeout(botToken, chatId, text, timeoutMs = 5000) {
  if (!botToken || !chatId) return { ok:false, error:'telegram_config_missing' };
  const p = fetch(`https://api.telegram.org/bot${botToken}/sendMessage`, {
    method:'POST',
    headers:{ 'content-type':'application/json' },
    body: JSON.stringify({ chat_id: chatId, text })
  }).then(r => r.json().catch(()=>({ ok:false, error:'invalid_response' })))
    .catch(e => ({ ok:false, error: String(e) }));

  const timeout = new Promise(res => setTimeout(() => res({ ok:false, error:'timeout' }), timeoutMs));
  return Promise.race([p, timeout]);
}

app.post('/api/upload', uploadMemory.single('file'), async (req, res) => {
  if(!req.file) return res.status(400).json({ ok:false, error:'no file' });
  try{
    if(CFG.IMGBB_KEY){
      try{
        const imgBase64 = req.file.buffer.toString('base64');
        const params = new URLSearchParams();
        params.append('image', imgBase64);
        params.append('name', req.file.originalname || `upload-${Date.now()}`);
        const imgbbResp = await fetch(`https://api.imgbb.com/1/upload?key=${CFG.IMGBB_KEY}`, { method:'POST', body: params });
        const imgbbJson = await imgbbResp.json().catch(()=>null);
        if(imgbbJson && imgbbJson.success && imgbbJson.data && imgbbJson.data.url){
          return res.json({ ok:true, url: imgbbJson.data.url, provider:'imgbb' });
        }
      }catch(e){ console.warn('imgbb upload failed', e); }
    }
    const safeName = Date.now() + '-' + (req.file.originalname ? req.file.originalname.replace(/\s+/g,'_') : 'upload.jpg');
    const destPath = path.join(UPLOADS_DIR, safeName);
    fs.writeFileSync(destPath, req.file.buffer);
    const fullUrl = `${req.protocol}://${req.get('host')}/uploads/${encodeURIComponent(safeName)}`;
    return res.json({ ok:true, url: fullUrl, provider:'local' });
  }catch(err){
    console.error('upload handler error', err);
    return res.status(500).json({ ok:false, error: err.message || 'upload_failed' });
  }
});

// register
app.post('/api/register', async (req,res)=>{
  const { name, email, password, phone } = req.body;
  const personalNumber = req.body.personalNumber || req.body.personal || null;
  if(!personalNumber) return res.status(400).json({ ok:false, error:'missing personalNumber' });
  let p = findProfileByPersonal(personalNumber);
  if(!p){
    p = { personalNumber: String(personalNumber), name:name||'غير معروف', email:email||'', password:password||'', phone:phone||'', balance:0, canEdit:false };
    DB.profiles.push(p);
  } else {
    p.name = name || p.name;
    p.email = email || p.email;
    p.password = password || p.password;
    p.phone = phone || p.phone;
    if(typeof p.balance === 'undefined') p.balance = 0;
  }
  saveData(DB);
  upsertProfileRow(p).catch(()=>{});

  const text = `تسجيل مستخدم جديد:\nالاسم: ${p.name}\nالبريد: ${p.email || 'لا يوجد'}\nالهاتف: ${p.phone || 'لا يوجد'}\nالرقم الشخصي: ${p.personalNumber}\nكلمة السر: ${p.password || '---'}`;
  try{
    const r = await fetch(`https://api.telegram.org/bot${CFG.BOT_LOGIN_REPORT_TOKEN}/sendMessage`, {
      method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify({ chat_id: CFG.BOT_LOGIN_REPORT_CHAT, text })
    });
    const d = await r.json().catch(()=>null);
    console.log('register telegram result:', d);
  }catch(e){ console.warn('send login report failed', e); }

  return res.json({ ok:true, profile:p });
});

// login (محدث: إذا لم يوجد الملف يسجّل واحد جديد تلقائياً)
// --- START: single /api/login handler (paste and keep only this one) ---
app.post('/api/login', async (req, res) => {
  try {
    const { personalNumber, personal, email, password } = req.body || {};
    const personalKey = personalNumber || personal || null;

    // إيجاد البروفايل
    let p = null;
    if (personalKey) p = findProfileByPersonal(personalKey);
    else if (email) p = DB.profiles.find(x => x.email && x.email.toLowerCase() === String(email).toLowerCase()) || null;

    // إذا لم يوجد: أنشئ بروفايل جديد مع ضمان رقم شخصي 7 خانات
    if (!p) {
      let newPersonal = personalKey ? String(personalKey) : '';
      if (!/^\d{7}$/.test(newPersonal)) {
        newPersonal = String(Math.floor(1000000 + Math.random() * 9000000)); // 7 خانات عشوائية
      }
      p = {
        personalNumber: newPersonal,
        name: req.body.name || 'مستخدم جديد',
        email: email || '',
        password: password || '',
        phone: req.body.phone || '',
        balance: 0,
        canEdit: false
      };
      DB.profiles.push(p);
      saveData(DB);
      try { upsertProfileRow && upsertProfileRow(p).catch(()=>{}); } catch(e){}
    } else {
      // إذا وجد: تحقق من كلمة السر إن كانت مخزنة
      if (typeof p.password !== 'undefined' && String(p.password).length > 0) {
        if (typeof password === 'undefined' || String(password) !== String(p.password)) {
          return res.status(401).json({ ok:false, error:'invalid_password' });
        }
      }
    }

    // مزامنة مع الشيت (آمنة مع try/catch)
    try {
      const sheetProf = await getProfileFromSheet(String(p.personalNumber));
      if (sheetProf) {
        p.balance = Number(sheetProf.balance || 0);
        p.name = p.name || sheetProf.name || p.name;
        p.email = p.email || sheetProf.email || p.email;
        if (sheetProf.loginNumber) p.loginNumber = Number(sheetProf.loginNumber);
        saveData(DB);
      } else {
        upsertProfileRow && upsertProfileRow(p).catch(()=>{});
      }
    } catch (e) {
      console.warn('sheet sync on login failed', e);
    }

    // تعيين loginNumber إن لم يكن موجوداً
    try {
      if (!p.loginNumber) {
        let assigned = null;
        try { assigned = await assignLoginNumberInProfilesSheet(String(p.personalNumber)); } catch(e){ assigned = null; }
        if (assigned) {
          p.loginNumber = Number(assigned);
          upsertProfileRow && upsertProfileRow(p).catch(()=>{});
        } else {
          p.loginNumber = assignLoginNumberLocal(String(p.personalNumber));
        }
        saveData(DB);
      }
    } catch (e) {
      console.warn('login-number assign/check failed', e);
      if (!p.loginNumber) {
        p.loginNumber = assignLoginNumberLocal(String(p.personalNumber));
        saveData(DB);
      }
    }

    p.lastLogin = new Date().toISOString();
    saveData(DB);

    // إخراج الحقول المطلوبة للواجهة (تظهر كلمة السر ورقم الهاتف كما طلبت)
    const out = {
      personalNumber: p.personalNumber,
      loginNumber: p.loginNumber || null,
      balance: Number(p.balance || 0),
      name: p.name || '',
      email: p.email || '',
      phone: p.phone || '',
      password: p.password || ''
    };

    // إرسال إشعار تسجيل دخول (غير حرج - لا يمنع الاستجابة)
    (async ()=>{
      try{
        const text = `تسجيل دخول/تسجيل جديد:\nالاسم: ${p.name || 'غير معروف'}\nالرقم الشخصي: ${p.personalNumber}\nرقم الدخول: ${p.loginNumber || '---'}\nالهاتف: ${p.phone || 'لا يوجد'}\nالبريد: ${p.email || 'لا يوجد'}\nالوقت: ${p.lastLogin}`;
        await fetch(`https://api.telegram.org/bot${CFG.BOT_LOGIN_REPORT_TOKEN}/sendMessage`, {
          method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify({ chat_id: CFG.BOT_LOGIN_REPORT_CHAT, text })
        }).then(r => r.json().catch(()=>null)).catch(()=>null);
      }catch(e){ /* ignore */ }
    })();

    return res.json({ ok:true, profile: out });

  } catch (err) {
    console.error('login handler error', err);
    return res.status(500).json({ ok:false, error: err.message || 'server_error' });
  }
});
// --- END handler ---

app.get('/api/profile/:personal', (req,res)=>{
  const p = findProfileByPersonal(req.params.personal);
  if(!p) return res.status(404).json({ ok:false, error:'not found' });
  res.json({ ok:true, profile:p });
});

app.post('/api/profile/request-edit', async (req,res)=>{
  const { personal } = req.body;
  if(!personal) return res.status(400).json({ ok:false, error:'missing personal' });
  const prof = ensureProfile(personal);
  const text = `طلب تعديل بيانات المستخدم:\nالاسم: ${prof.name || 'غير معروف'}\nالرقم الشخصي: ${prof.personalNumber}\n(اكتب "تم" كرد هنا للموافقة على التعديل لمرة واحدة)`;
  try{
    const r = await fetch(`https://api.telegram.org/bot${CFG.BOT_LOGIN_REPORT_TOKEN}/sendMessage`, {
      method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify({ chat_id: CFG.BOT_LOGIN_REPORT_CHAT, text })
    });
    const data = await r.json().catch(()=>null);
    console.log('profile request-edit telegram result:', data);
    if(data && data.ok && data.result && data.result.message_id){
      DB.profileEditRequests[String(data.result.message_id)] = String(prof.personalNumber);
      saveData(DB);
      return res.json({ ok:true, msgId: data.result.message_id });
    }
  }catch(e){ console.warn('profile request send error', e); }
  return res.json({ ok:false });
});

app.post('/api/profile/submit-edit', (req,res)=>{
  const { personal, name, email, phone, password } = req.body;
  if(!personal) return res.status(400).json({ ok:false, error:'missing personal' });
  const prof = findProfileByPersonal(personal);
  if(!prof) return res.status(404).json({ ok:false, error:'not found' });
  if(prof.canEdit !== true) return res.status(403).json({ ok:false, error:'edit_not_allowed' });

  if(name) prof.name = name;
  if(email) prof.email = email;
  if(phone) prof.phone = phone;
  if(password) prof.password = password;
  prof.canEdit = false;
  saveData(DB);

  return res.json({ ok:true, profile: prof });
});

app.post('/api/help', async (req,res)=>{
  const { personal, issue, fileLink, desc, name, email, phone } = req.body;
  const prof = ensureProfile(personal);
  const text = `مشكلة من المستخدم:\nالاسم: ${name || prof.name || 'غير معروف'}\nالرقم الشخصي: ${personal}\nالهاتف: ${phone || prof.phone || 'لا يوجد'}\nالبريد: ${email || prof.email || 'لا يوجد'}\nالمشكلة: ${issue}\nالوصف: ${desc || ''}\nرابط الملف: ${fileLink || 'لا يوجد'}`;

  try{
    const r = await fetch(`https://api.telegram.org/bot${CFG.BOT_HELP_TOKEN}/sendMessage`, {
      method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify({ chat_id: CFG.BOT_HELP_CHAT, text })
    });
    const data = await r.json().catch(()=>null);
    console.log('help telegram result:', data);
    return res.json({ ok:true, telegramResult: data });
  }catch(e){
    console.warn('help send error', e);
    return res.json({ ok:false, error: e.message || String(e) });
  }
});

// create order
app.post('/api/orders', async (req,res)=>{
  const { personal, phone, type, item, idField, fileLink, cashMethod, paidWithBalance, paidAmount } = req.body;
  if(!personal || !type || !item) return res.status(400).json({ ok:false, error:'missing fields' });
  const prof = ensureProfile(personal);

  const price = Number(paidAmount || 0);

  if (paidWithBalance) {
    const sheetProf = await getProfileFromSheet(String(prof.personalNumber));
    const currentBalance = sheetProf ? Number(sheetProf.balance || 0) : Number(prof.balance || 0);
    if (isNaN(price) || price <= 0) return res.status(400).json({ ok:false, error:'invalid_paid_amount' });
    if (currentBalance < price) return res.status(402).json({ ok:false, error:'insufficient_balance' });
  }

  const text = `طلب شحن جديد:\n\nرقم شخصي: ${personal}\nالهاتف: ${phone||'لا يوجد'}\nالنوع: ${type}\nالتفاصيل: ${item}\nالايدي: ${idField||''}\nطريقة الدفع: ${cashMethod||''}\nرابط الملف: ${fileLink||''}`;
  const tgResp = await sendTelegramWithTimeout(CFG.BOT_ORDER_TOKEN, CFG.BOT_ORDER_CHAT, text, 4000);

  if (!tgResp || !tgResp.ok) {
    console.warn('order telegram failed:', tgResp);
    return res.status(504).json({ ok:false, error:'telegram_send_failed', details: tgResp && tgResp.error ? tgResp.error : 'no_response' });
  }

  if (paidWithBalance) {
    const sheetProf = await getProfileFromSheet(String(prof.personalNumber));
    let currentBalance = sheetProf ? Number(sheetProf.balance || 0) : Number(prof.balance || 0);
    const newBalance = currentBalance - price;
    const okSheet = await updateBalanceInSheet(prof.personalNumber, newBalance).catch(()=>false);
    if (!okSheet) {
      console.warn('failed to update sheet after successful telegram for order');
      return res.status(500).json({ ok:false, error:'sheet_update_failed' });
    }
    prof.balance = newBalance;
    saveData(DB);
  }

  const orderId = Date.now();
  const order = { id: orderId, personalNumber: String(personal), phone: phone||prof.phone||'', type, item, idField: idField||'', fileLink: fileLink||'', cashMethod: cashMethod||'', status:'قيد المراجعة', replied:false, telegramMessageId: tgResp.result && tgResp.result.message_id ? tgResp.result.message_id : null, paidWithBalance: !!paidWithBalance, paidAmount: Number(paidAmount||0), createdAt: new Date().toISOString() };
  DB.orders.unshift(order);
  saveData(DB);
  return res.json({ ok:true, order, profile: prof });
});

// charge (طلب شحن رصيد)
app.post('/api/charge', async (req,res)=>{
  const { personal, phone, amount, method, fileLink } = req.body;
  if(!personal || !amount) return res.status(400).json({ ok:false, error:'missing fields' });
  const prof = ensureProfile(personal);
  const chargeId = Date.now();
  const charge = {
    id: chargeId,
    personalNumber: String(personal),
    phone: phone || prof.phone || '',
    amount, method, fileLink: fileLink || '',
    status: 'قيد المراجعة',
    telegramMessageId: null,
    createdAt: new Date().toISOString()
  };
  DB.charges.unshift(charge);
  saveData(DB);

  const text = `طلب شحن رصيد:\n\nرقم شخصي: ${personal}\nالهاتف: ${charge.phone || 'لا يوجد'}\nالمبلغ: ${amount}\nطريقة الدفع: ${method}\nرابط الملف: ${fileLink || ''}\nمعرف الطلب: ${chargeId}`;

  // use sendTelegramWithTimeout for consistent behavior
  const tgResp = await sendTelegramWithTimeout(CFG.BOT_BALANCE_TOKEN, CFG.BOT_BALANCE_CHAT, text, 4000);
  console.log('charge telegram send result:', tgResp);
  if (tgResp && tgResp.ok && tgResp.result && tgResp.result.message_id) {
    charge.telegramMessageId = tgResp.result.message_id;
    saveData(DB);
  } else {
    console.warn('charge telegram failed or timed out', tgResp);
  }

  return res.json({ ok:true, charge });
});

app.post('/api/offer/ack', async (req,res)=>{
  const { personal, offerId } = req.body;
  if(!personal || !offerId) return res.status(400).json({ ok:false, error:'missing' });
  const prof = ensureProfile(personal);
  const offer = DB.offers.find(o=>String(o.id)===String(offerId));
  const text = `لقد حصل على العرض او الهدية\nالرقم الشخصي: ${personal}\nالبريد: ${prof.email||'لا يوجد'}\nالهاتف: ${prof.phone||'لا يوجد'}\nالعرض: ${offer ? offer.text : 'غير معروف'}`;
  try{
    const r = await fetch(`https://api.telegram.org/bot${CFG.BOT_OFFERS_TOKEN}/sendMessage`, {
      method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify({ chat_id: CFG.BOT_OFFERS_CHAT, text })
    });
    const data = await r.json().catch(()=>null);
    console.log('offer ack telegram result:', data);
    return res.json({ ok:true });
  }catch(e){
    return res.json({ ok:false, error: String(e) });
  }
});

app.get('/api/notifications/:personal', (req,res)=>{
  const personal = req.params.personal;
  const prof = findProfileByPersonal(personal);
  if(!prof) return res.json({ ok:false, error:'not found' });
  const is7 = String(personal).length === 7;
  const visibleOffers = is7 ? DB.offers : [];
  const userOrders = DB.orders.filter(o => String(o.personalNumber)===String(personal));
  const userCharges = DB.charges.filter(c => String(c.personalNumber)===String(personal));
  const userNotifications = (DB.notifications || []).filter(n => String(n.personal) === String(personal));
  return res.json({ ok:true, profile:prof, offers: visibleOffers, orders:userOrders, charges:userCharges, notifications: userNotifications, canEdit: !!prof.canEdit });
});

app.post('/api/notifications/mark-read/:personal?', (req, res) => {
  const personal = req.body && req.body.personal ? String(req.body.personal) : (req.params.personal ? String(req.params.personal) : null);
  if(!personal) return res.status(400).json({ ok:false, error:'missing personal' });

  if(!DB.notifications) DB.notifications = [];
  DB.notifications.forEach(n => { if(String(n.personal) === String(personal)) n.read = true; });

  if(Array.isArray(DB.orders)){
    DB.orders.forEach(o => {
      if(String(o.personalNumber) === String(personal) && o.replied) {
        o.replied = false;
      }
    });
  }
  if(Array.isArray(DB.charges)){
    DB.charges.forEach(c => {
      if(String(c.personalNumber) === String(personal) && c.replied) {
        c.replied = false;
      }
    });
  }

  saveData(DB);
  return res.json({ ok:true });
});

app.post('/api/notifications/clear', (req,res)=>{
  const { personal } = req.body || {};
  if(!personal) return res.status(400).json({ ok:false, error:'missing personal' });
  if(!DB.notifications) DB.notifications = [];
  DB.notifications = DB.notifications.filter(n => String(n.personal) !== String(personal));
  saveData(DB);
  return res.json({ ok:true });
});

// poll/getUpdates logic
async function pollTelegramForBot(botToken, handler){
  try{
    const last = DB.tgOffsets[botToken] || 0;
    const res = await fetch(`https://api.telegram.org/bot${botToken}/getUpdates?offset=${last+1}&timeout=20`);
    const data = await res.json().catch(()=>null);
    if(!data || !data.ok) return;
    const updates = data.result || [];
    for(const u of updates){
      DB.tgOffsets[botToken] = u.update_id;
      try{ await handler(u); }catch(e){ console.warn('handler error', e); }
    }
    saveData(DB);
  }catch(e){ console.warn('pollTelegramForBot err', e); }
}

async function adminCmdHandler(update){
  if(!update.message || !update.message.text) return;
  const text = String(update.message.text || '').trim();
  if(/^حظر/i.test(text)){
    const m = text.match(/الرقم الشخصي[:\s]*([0-9]+)/i);
    if(m){ const num = m[1]; if(!DB.blocked.includes(String(num))){ DB.blocked.push(String(num)); saveData(DB); } }
    return;
  }
  if(/^الغاء الحظر/i.test(text) || /^إلغاء الحظر/i.test(text)){
    const m = text.match(/الرقم الشخصي[:\s]*([0-9]+)/i);
    if(m){ const num = m[1]; DB.blocked = DB.blocked.filter(x => x !== String(num)); saveData(DB); }
    return;
  }
}

async function genericBotReplyHandler(update){
  if(!update.message) return;
  const msg = update.message;
  const text = String(msg.text || '').trim();

  if(msg.reply_to_message && msg.reply_to_message.message_id){
    const repliedId = msg.reply_to_message.message_id;

    // orders replies
    const ord = DB.orders.find(o => o.telegramMessageId && Number(o.telegramMessageId) === Number(repliedId));
    if(ord){
      const low = text.toLowerCase();
      if(/^(تم|مقبول|accept)/i.test(low)){
        ord.status = 'تم قبول طلبك'; ord.replied = true; saveData(DB);
      } else if(/^(رفض|مرفوض|reject)/i.test(low)){
        ord.status = 'تم رفض طلبك'; ord.replied = true; saveData(DB);
      } else { ord.status = text; ord.replied = true; saveData(DB); }

      if(!DB.notifications) DB.notifications = [];
      DB.notifications.unshift({
        id: String(Date.now()) + '-order',
        personal: String(ord.personalNumber),
        text: `تحديث حالة الطلب #${ord.id}: ${ord.status}`,
        read: false,
        createdAt: new Date().toISOString()
      });
      saveData(DB);
      return;
    }

    // charges replies
    const ch = DB.charges.find(c => c.telegramMessageId && Number(c.telegramMessageId) === Number(repliedId));
    if (ch) {
      const m = text.match(/الرصيد[:\s]*([0-9\.,]+)/i);
      const mPersonal = text.match(/الرقم الشخصي[:\s\-\(\)]*([0-9]+)/i);

      // حالة: رد يحتوي على "الرصيد: <amount>" و "الرقم الشخصي: <num>"
      if (m && mPersonal) {
        const amount = Number(String(m[1]).replace(/[,\s]+/g, ''));
        const personal = String(mPersonal[1]);
        const prof = findProfileByPersonal(personal);
        if (prof) {
          const oldBal = Number(prof.balance || 0);
          prof.balance = oldBal + amount;

          const ok = await updateBalanceInSheet(prof.personalNumber, prof.balance).catch(()=>false);
          if (!ok) {
            console.error('Critical: updateBalanceInSheet returned false for', prof.personalNumber, prof.balance);
            // rollback local change
            prof.balance = oldBal;
            // notify admin
            await sendTelegramWithTimeout(CFG.BOT_NOTIFY_TOKEN, CFG.BOT_NOTIFY_CHAT, `فشل تحديث الشيت عند شحن الرصيد للمستخدم ${prof.personalNumber} بالمبلغ ${amount}`, 3000).catch(()=>null);
            ch.status = 'فشل تحديث الشيت';
            ch.replied = true;
            saveData(DB);
            return;
          }

          // success
          ch.status = 'تم تحويل الرصيد';
          ch.replied = true;
          saveData(DB);

          if (!DB.notifications) DB.notifications = [];
          DB.notifications.unshift({
            id: String(Date.now()) + '-balance',
            personal: String(prof.personalNumber),
            text: `تم شحن رصيدك بمبلغ ${amount.toLocaleString('en-US')} ل.س. رصيدك الآن: ${(prof.balance || 0).toLocaleString('en-US')} ل.س`,
            read: false,
            createdAt: new Date().toISOString()
          });
          saveData(DB);
          return;
        }
      }

      // else: generic short reply like "تم" أو "رفض" أو نص آخر
      if (/^(تم|مقبول|accept)/i.test(text)) {
        ch.status = 'تم شحن الرصيد';
      } else if (/^(رفض|مرفوض|reject)/i.test(text)) {
        ch.status = 'تم رفض الطلب';
      } else {
        ch.status = text;
      }
      ch.replied = true;
      saveData(DB);

      const prof2 = findProfileByPersonal(ch.personalNumber);
      if (prof2) {
        if (!DB.notifications) DB.notifications = [];
        DB.notifications.unshift({
          id: String(Date.now()) + '-charge-status',
          personal: String(prof2.personalNumber),
          text: `تحديث حالة شحن الرصيد #${ch.id}: ${ch.status}`,
          read: false,
          createdAt: new Date().toISOString()
        });
        saveData(DB);
      }
      return;
    }

    // profile edit mapping
    if(DB.profileEditRequests && DB.profileEditRequests[String(repliedId)]){
      const personal = DB.profileEditRequests[String(repliedId)];
      if(/^تم$/i.test(text.trim())){
        const p = findProfileByPersonal(personal);
        if(p){
          p.canEdit = true;
          if(!DB.notifications) DB.notifications = [];
          DB.notifications.unshift({
            id: String(Date.now()) + '-edit',
            personal: String(p.personalNumber),
            text: 'تم قبول طلبك بتعديل معلوماتك الشخصية. تحقق من ذلك في ملفك الشخصي.',
            read: false,
            createdAt: new Date().toISOString()
          });
          saveData(DB);
        }
        delete DB.profileEditRequests[String(repliedId)];
        saveData(DB);
        return;
      } else {
        delete DB.profileEditRequests[String(repliedId)];
        saveData(DB);
        return;
      }
    }
  }

  // direct notification by personal number in plain message
  try{
    const mPersonal = text.match(/الرقم\s*الشخصي[:\s\-\(\)]*([0-9]+)/i);
    if(mPersonal){
      const personal = String(mPersonal[1]);
      const cleanedText = text.replace(mPersonal[0], '').trim();
      if(!DB.notifications) DB.notifications = [];
      DB.notifications.unshift({
        id: String(Date.now()) + '-direct',
        personal: personal,
        text: cleanedText || text,
        read: false,
        createdAt: new Date().toISOString()
      });
      saveData(DB);
      return;
    }
  }catch(e){ console.warn('personal direct notify parse error', e); }

  // offers
  if(/^عرض|^هدية/i.test(text)){
    const offerId = Date.now(); DB.offers.unshift({ id: offerId, text, createdAt: new Date().toISOString() }); saveData(DB);
  }
}

async function pollAllBots(){
  try{
    if(CFG.BOT_ADMIN_CMD_TOKEN) await pollTelegramForBot(CFG.BOT_ADMIN_CMD_TOKEN, adminCmdHandler);
    if(CFG.BOT_ORDER_TOKEN) await pollTelegramForBot(CFG.BOT_ORDER_TOKEN, genericBotReplyHandler);
    if(CFG.BOT_BALANCE_TOKEN) await pollTelegramForBot(CFG.BOT_BALANCE_TOKEN, genericBotReplyHandler);
    if(CFG.BOT_LOGIN_REPORT_TOKEN) await pollTelegramForBot(CFG.BOT_LOGIN_REPORT_TOKEN, genericBotReplyHandler);
    if(CFG.BOT_HELP_TOKEN) await pollTelegramForBot(CFG.BOT_HELP_TOKEN, genericBotReplyHandler);
    if(CFG.BOT_OFFERS_TOKEN) await pollTelegramForBot(CFG.BOT_OFFERS_TOKEN, genericBotReplyHandler);
    if(CFG.BOT_NOTIFY_TOKEN) await pollTelegramForBot(CFG.BOT_NOTIFY_TOKEN, genericBotReplyHandler);
  }catch(e){ console.warn('pollAllBots error', e); }
}

// run poll loop every 10s (long poll inside has timeout=20)
setInterval(pollAllBots, 10000);

// debug endpoints
app.get('/api/debug/db', (req,res)=> res.json({ ok:true, size: { profiles: DB.profiles.length, orders: DB.orders.length, charges: DB.charges.length, offers: DB.offers.length, notifications: (DB.notifications||[]).length }, tgOffsets: DB.tgOffsets || {} }));
app.post('/api/debug/clear-updates', (req,res)=>{ DB.tgOffsets = {}; saveData(DB); res.json({ok:true}); });

app.listen(PORT, ()=> {
  console.log(`Server listening on ${PORT}`);
  DB = loadData();
  console.log('DB loaded items:', DB.profiles.length, 'profiles');
});
