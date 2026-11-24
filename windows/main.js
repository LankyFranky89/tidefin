// Tidefin for Windows: a window onto Tidefin on the NAS, with media keys, tray and optional autostart.
const { app, BrowserWindow, Tray, Menu, shell, nativeImage } = require('electron');
const path = require('path');
const fs = require('fs');

if (!app.requestSingleInstanceLock()) { app.quit(); process.exit(0); }
app.setAppUserModelId('no.techfix.tidefin');

const CFG = () => path.join(app.getPath('userData'), 'config.json');
let cfg = { server: '', closeToTray: true };
try { cfg = { ...cfg, ...JSON.parse(fs.readFileSync(CFG(), 'utf8')) }; } catch {}
const saveCfg = () => { try { fs.writeFileSync(CFG(), JSON.stringify(cfg, null, 2)); } catch {} };

let win = null, tray = null, quitting = false;
const startHidden = process.argv.includes('--hidden');

function showSetup(error) {
  win.loadFile(path.join(__dirname, 'setup.html'), { query: { server: cfg.server || 'http://192.168.1.185:8585', error: error || '' } });
}
function openServer() {
  if (!cfg.server) return showSetup();
  win.loadURL(cfg.server).catch(() => {});
}
const run = (js) => win && win.webContents.executeJavaScript(js).catch(() => {});
const click = (id) => run(`(document.getElementById('${id}')||{click(){}}).click()`);

function createWindow() {
  win = new BrowserWindow({
    width: 1280, height: 820, minWidth: 420, minHeight: 500,
    backgroundColor: '#0b0f14', title: 'Tidefin', icon: path.join(__dirname, 'build/icon.ico'),
    autoHideMenuBar: true, show: !startHidden,
    webPreferences: { backgroundThrottling: false, contextIsolation: true, sandbox: true },
  });
  win.setMenuBarVisibility(false);
  // Setup page saves the address by navigating to tidefin-save:?url=...
  win.webContents.on('will-navigate', (e, url) => {
    if (url.startsWith('tidefin-save:')) {
      e.preventDefault();
      const u = new URL(url).searchParams.get('url') || '';
      cfg.server = u.replace(/\/+$/, ''); saveCfg(); openServer();
    }
  });
  win.webContents.on('did-fail-load', (_e, code, desc, url, isMain) => {
    if (isMain && code !== -3 && !url.startsWith('file:')) showSetup(`Couldn't reach ${cfg.server} (${desc}). Is the NAS on, and are you on the home network or Tailscale?`);
  });
  // Links to other sites open in the normal browser
  win.webContents.setWindowOpenHandler(({ url }) => { shell.openExternal(url); return { action: 'deny' }; });
  win.on('close', (e) => { if (!quitting && cfg.closeToTray) { e.preventDefault(); win.hide(); } });
  openServer();
}

function buildTray() {
  tray = new Tray(nativeImage.createFromPath(path.join(__dirname, 'build/tray.png')));
  tray.setToolTip('Tidefin');
  const menu = () => Menu.buildFromTemplate([
    { label: 'Show Tidefin', click: () => { win.show(); win.focus(); } },
    { type: 'separator' },
    { label: 'Play / Pause', click: () => click('pp') },
    { label: 'Next', click: () => click('next') },
    { label: 'Previous', click: () => click('prev') },
    { type: 'separator' },
    { label: 'Start with Windows', type: 'checkbox', checked: app.getLoginItemSettings().openAtLogin,
      click: (m) => app.setLoginItemSettings({ openAtLogin: m.checked, args: ['--hidden'] }) },
    { label: 'Keep playing in tray when closed', type: 'checkbox', checked: cfg.closeToTray,
      click: (m) => { cfg.closeToTray = m.checked; saveCfg(); } },
    { label: 'Change NAS address…', click: () => { win.show(); showSetup(); } },
    { type: 'separator' },
    { label: 'Quit', click: () => { quitting = true; app.quit(); } },
  ]);
  tray.setContextMenu(menu());
  tray.on('right-click', () => tray.setContextMenu(menu()));
  tray.on('click', () => { if (win.isVisible() && win.isFocused()) win.hide(); else { win.show(); win.focus(); } });
}

app.on('second-instance', () => { if (win) { win.show(); if (win.isMinimized()) win.restore(); win.focus(); } });
app.on('before-quit', () => { quitting = true; });
app.whenReady().then(() => { createWindow(); buildTray(); });
app.on('window-all-closed', () => { if (!cfg.closeToTray) app.quit(); });
