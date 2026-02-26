/* ============================================================
   TempMail SPA — 主应用逻辑
   ============================================================ */

'use strict';

// ─── 配置 ───────────────────────────────────────────────────
const API_BASE = '/api';
const PUBLIC_BASE = '/public';

// ─── 状态 ───────────────────────────────────────────────────
const state = {
  apiKey:    localStorage.getItem('tm_apikey') || '',
  account:   JSON.parse(localStorage.getItem('tm_account') || 'null'),
  theme:     localStorage.getItem('tm_theme') || 'light',
  page:      'dashboard',
  // 当前邮箱
  currentMailbox: null,
  currentEmail:   null,
  // 缓存
  mailboxes: [],
  emails:    [],
};

// ─── 工具函数 ───────────────────────────────────────────────
const $ = id => document.getElementById(id);
const el = (tag, cls, html) => {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (html !== undefined) e.innerHTML = html;
  return e;
};

function toast(msg, type = 'info') {
  const icons = { success: '✓', error: '✗', warn: '⚠', info: 'ℹ' };
  const t = el('div', `toast ${type}`, `<span>${icons[type]||'ℹ'}</span><span>${escHtml(msg)}</span>`);
  const c = $('toast-container');
  c.appendChild(t);
  setTimeout(() => { t.style.opacity = '0'; t.style.transition = 'opacity 0.3s'; setTimeout(() => t.remove(), 300); }, 3500);
}

function escHtml(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

function formatDate(s) {
  if (!s) return '—';
  const d = new Date(s);
  return d.toLocaleString('zh-CN', { month:'2-digit', day:'2-digit', hour:'2-digit', minute:'2-digit'});
}

function timeAgo(s) {
  if (!s) return '—';
  const diff = Date.now() - new Date(s).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return '刚刚';
  if (mins < 60) return `${mins}分钟前`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}小时前`;
  return `${Math.floor(hrs/24)}天前`;
}

async function copyText(text) {
  try {
    await navigator.clipboard.writeText(text);
    toast('已复制到剪贴板', 'success');
  } catch {
    toast('复制失败，请手动选择', 'warn');
  }
}

// ─── API 客户端 ─────────────────────────────────────────────
async function apiFetch(path, opts = {}) {
  const headers = { 'Content-Type': 'application/json', ...(opts.headers || {}) };
  if (state.apiKey) headers['Authorization'] = `Bearer ${state.apiKey}`;
  const res = await fetch(path, { ...opts, headers });
  let data;
  try { data = await res.json(); } catch { data = {}; }
  if (!res.ok) {
    const errMsg = data.error || data.message || `HTTP ${res.status}`;
    throw new Error(errMsg);
  }
  return data;
}

const api = {
  // 公共
  publicSettings: () => fetch(PUBLIC_BASE + '/settings').then(r => r.json()),
  publicStats:     () => fetch(PUBLIC_BASE + '/stats').then(r => r.json()),
  register: body  => apiFetch(PUBLIC_BASE + '/register', { method: 'POST', body: JSON.stringify(body) }),

  // 账户
  me:              () => apiFetch(API_BASE + '/me'),
  stats:           () => apiFetch(API_BASE + '/stats'),
  // 域名 → 解包 {domains:[...]} → 数组
  domains:         () => apiFetch(API_BASE + '/domains').then(d => Array.isArray(d) ? d : (d.domains || [])),
  // 任意已登录用户提交域名 MX 验证
  submitDomain:    body => apiFetch(API_BASE + '/domains/submit', { method: 'POST', body: JSON.stringify(body) }),
  // 轮询域名状态（任意已登录用户，不需要管理员权限）
  getDomainStatus: id => apiFetch(API_BASE + '/domains/' + id + '/status'),
  // 邮箱 → 解包 {data:[...]}
  createMailbox:   (body) => apiFetch(API_BASE + '/mailboxes', { method: 'POST', body: JSON.stringify(body || {}) }).then(d => d.mailbox || d),
  listMailboxes:   () => apiFetch(API_BASE + '/mailboxes').then(d => Array.isArray(d) ? d : (d.data || [])),
  deleteMailbox: id  => apiFetch(API_BASE + '/mailboxes/' + id, { method: 'DELETE' }),
  // 邮件 → 解包 {data:[...]}
  listEmails: mid    => apiFetch(API_BASE + '/mailboxes/' + mid + '/emails').then(d => Array.isArray(d) ? d : (d.data || [])),
  getEmail:   (mid, eid) => apiFetch(API_BASE + '/mailboxes/' + mid + '/emails/' + eid).then(d => d.email || d),
  deleteEmail:(mid, eid) => apiFetch(API_BASE + '/mailboxes/' + mid + '/emails/' + eid, { method: 'DELETE' }),
  // 管理
  admin: {
    listAccounts:  (page=1,size=50) => apiFetch(API_BASE + '/admin/accounts?page='+page+'&size='+size).then(d => Array.isArray(d) ? d : (d.data || [])),
    createAccount: body => apiFetch(API_BASE + '/admin/accounts', { method: 'POST', body: JSON.stringify(body) }),
    deleteAccount: id   => apiFetch(API_BASE + '/admin/accounts/' + id, { method: 'DELETE' }),
    addDomain:   body => apiFetch(API_BASE + '/admin/domains', { method: 'POST', body: JSON.stringify(body) }),
    deleteDomain:  id => apiFetch(API_BASE + '/admin/domains/' + id, { method: 'DELETE' }),
    toggleDomain:  (id, active) => apiFetch(API_BASE + '/admin/domains/' + id + '/toggle', { method: 'PUT', body: JSON.stringify({ active }) }),
    getSettings:    () => apiFetch(API_BASE + '/admin/settings'),
    saveSettings: body => apiFetch(API_BASE + '/admin/settings', { method: 'PUT', body: JSON.stringify(body) }),
    mxImport:    body => apiFetch(API_BASE + '/admin/domains/mx-import', { method: 'POST', body: JSON.stringify(body) }),
    mxRegister:  body => apiFetch(API_BASE + '/admin/domains/mx-register', { method: 'POST', body: JSON.stringify(body) }),
    getDomainStatus: id => apiFetch(API_BASE + '/admin/domains/' + id + '/status'),
  },
};

// ─── 主题 ────────────────────────────────────────────────────
function applyTheme(t) {
  document.documentElement.dataset.theme = t;
  state.theme = t;
  localStorage.setItem('tm_theme', t);
  const btn = $('btn-theme');
  if (btn) btn.textContent = t === 'dark' ? '☀ 浅色' : '☾ 深色';
}

// ─── 认证 ─────────────────────────────────────────────────────
async function tryLogin(key) {
  state.apiKey = key;
  try {
    const acct = await apiFetch(API_BASE + '/me');
    state.account = acct;
    localStorage.setItem('tm_apikey', key);
    localStorage.setItem('tm_account', JSON.stringify(acct));
    showMainLayout();
    navigate('dashboard');
    toast(`欢迎回来，${acct.username || '用户'}`, 'success');
  } catch (e) {
    state.apiKey = '';
    toast('API Key 无效: ' + e.message, 'error');
  }
}

function logout() {
  state.apiKey = '';
  state.account = null;
  localStorage.removeItem('tm_apikey');
  localStorage.removeItem('tm_account');
  showAuthPage();
}

// ─── 路由 ─────────────────────────────────────────────────────
function navigate(page, params = {}) {
  closeSidebar();
  // 离开收件箱时停止自动刷新
  if (page !== 'inbox') clearInboxPoller();
  state.page = page;
  Object.assign(state, params);
  renderPage(page);
  // 更新侧导航高亮
  document.querySelectorAll('.nav-item').forEach(n => {
    n.classList.toggle('active', n.dataset.page === page);
  });
}

// ─── 布局渲染 ──────────────────────────────────────────────────
function showAuthPage() {
  $('app').innerHTML = '';
  $('app').appendChild(buildAuthPage());
  renderLoginForm();
}

function showMainLayout() {
  $('app').innerHTML = '';
  $('app').appendChild(buildMainLayout());
  applyTheme(state.theme);
}

function buildAuthPage() {
  const wrap = el('div', null);
  wrap.id = 'auth-page';

  const card = el('div', 'auth-card');
  card.innerHTML = `
    <div class="auth-logo">
      <div class="logo-icon">✉</div>
      <h1>TempMail</h1>
      <p>临时邮箱服务 · 安全隔离 · 按需分配</p>
    </div>
    <div class="auth-tabs">
      <button class="auth-tab active" id="tab-login" onclick="switchAuthTab('login')">使用 API Key 登录</button>
      <button class="auth-tab" id="tab-reg" onclick="switchAuthTab('reg')">注册账户</button>
    </div>
    <div id="auth-form-area"></div>
  `;
  wrap.appendChild(card);

  // 检查是否允许注册
  api.publicSettings().then(d => {
    const open = d.registration_open === 'true' || d.registration_open === true;
    if (!open) {
      const regTab = card.querySelector('#tab-reg');
      if (regTab) { regTab.disabled = true; regTab.title = '管理员已关闭注册'; }
    }
  }).catch(() => {});

  return wrap;
}

window.switchAuthTab = function(t) {
  document.querySelectorAll('.auth-tab').forEach(b => b.classList.remove('active'));
  if (t === 'login') {
    $('tab-login').classList.add('active');
    renderLoginForm();
  } else {
    $('tab-reg').classList.add('active');
    renderRegForm();
  }
};

function renderLoginForm() {
  const area = $('auth-form-area');
  if (!area) return;
  area.innerHTML = `
    <div class="form-group">
      <label class="form-label">API Key</label>
      <input class="form-input" id="login-key" type="password" placeholder="tm_xxxxxxxxxxxx" autocomplete="current-password" />
      <div class="form-hint">在邮箱管理后台获取的 API Key</div>
    </div>
    <button class="btn btn-primary" style="width:100%" onclick="doLogin()">登 录</button>
    <div class="divider"></div>
    <div style="text-align:center;font-size:0.78rem;color:var(--text-muted)">
      没有账户？联系管理员创建，或点击上方"注册账户"
    </div>
  `;
  const inp = $('login-key');
  if (inp) inp.addEventListener('keydown', e => { if (e.key === 'Enter') doLogin(); });
}

function renderRegForm() {
  const area = $('auth-form-area');
  if (!area) return;
  area.innerHTML = `
    <div class="form-group">
      <label class="form-label">用户名</label>
      <input class="form-input" id="reg-username" type="text" placeholder="your_name" />
    </div>
    <div class="form-group">
      <label class="form-label">邮箱（可选）</label>
      <input class="form-input" id="reg-email" type="email" placeholder="contact@example.com" />
    </div>
    <button class="btn btn-primary" style="width:100%" onclick="doRegister()">注 册</button>
  `;
}

window.doLogin = async function() {
  const key = ($('login-key')?.value || '').trim();
  if (!key) { toast('请输入 API Key', 'warn'); return; }
  await tryLogin(key);
};

window.doRegister = async function() {
  const username = ($('reg-username')?.value || '').trim();
  const email    = ($('reg-email')?.value || '').trim();
  if (!username) { toast('请输入用户名', 'warn'); return; }
  try {
    const result = await api.register({ username, email: email || undefined });
    // 显示成功
    const area = $('auth-form-area');
    area.innerHTML = `
      <div class="apikey-hero">
        <span class="big-icon">🎉</span>
        <h2>注册成功！</h2>
        <p>请保存您的 API Key，它不会再次显示。</p>
        <div class="code-box">
          <span id="new-key">${escHtml(result.api_key)}</span>
          <button class="copy-btn" onclick="copyText('${escHtml(result.api_key)}')" title="复制">⎘</button>
        </div>
        <button class="btn btn-success" style="margin-top:1.2rem;width:100%" onclick="tryLogin('${escHtml(result.api_key)}')">立即登录</button>
      </div>
    `;
  } catch(e) {
    toast('注册失败: ' + e.message, 'error');
  }
};

// ─── 主布局 ────────────────────────────────────────────────────
function buildMainLayout() {
  const layout = el('div', null);
  layout.id = 'main-layout';
  layout.style.display = 'flex';
  layout.style.flex = '1';

  const isAdmin = state.account?.is_admin;
  const username = state.account?.username || '用户';

  // sidebar
  layout.innerHTML = `
    <div class="sidebar-backdrop" id="sidebar-backdrop" onclick="closeSidebar()"></div>
    <nav class="sidebar" id="main-sidebar">
      <div class="sidebar-logo">
        <div class="logo-mark">✉</div>
        <div>
          <span>TempMail</span>
          <small>临时邮箱服务</small>
        </div>
      </div>
      <div class="sidebar-nav">
        <div class="nav-section">邮件</div>
        <button class="nav-item active" data-page="dashboard" onclick="navigate('dashboard')">
          <span class="nav-icon">⊞</span><span>邮箱总览</span>
        </button>
        <button class="nav-item" data-page="domains-guide" onclick="navigate('domains-guide')">
          <span class="nav-icon">◎</span><span>域名列表</span>
        </button>
        <button class="nav-item" data-page="api-docs" onclick="navigate('api-docs')">
          <span class="nav-icon">📖</span><span>API 文档</span>
        </button>
        ${isAdmin ? `
        <div class="nav-section">管理</div>
        <button class="nav-item" data-page="admin-accounts" onclick="navigate('admin-accounts')">
          <span class="nav-icon">👥</span><span>账户管理</span>
        </button>
        <button class="nav-item" data-page="admin-domains" onclick="navigate('admin-domains')">
          <span class="nav-icon">🌐</span><span>域名管理</span>
        </button>
        <button class="nav-item" data-page="admin-settings" onclick="navigate('admin-settings')">
          <span class="nav-icon">⚙</span><span>系统设置</span>
        </button>
        ` : ''}
      </div>
      <div class="sidebar-bottom">
        <div class="user-chip">
          <div class="user-avatar">${username.charAt(0).toUpperCase()}</div>
          <div class="user-chip-info">
            <div class="user-chip-name">${escHtml(username)}</div>
            <div class="user-chip-role">${isAdmin ? '管理员' : '普通用户'}</div>
          </div>
        </div>
        <button class="btn-logout" onclick="logout()">⏏ 退出登录</button>
        <button class="btn-theme" id="btn-theme" onclick="toggleTheme()">${state.theme==='dark'?'☀ 浅色':'☾ 深色'}</button>
      </div>
    </nav>
    <div class="content" id="content-area">
      <div class="topbar">
        <div>
          <button class="hamburger-btn" id="hamburger-btn" onclick="toggleSidebar()" aria-label="菜单">☰</button>
          <div>
            <div class="topbar-title" id="topbar-title">邮箱总览</div>
            <div class="topbar-subtitle" id="topbar-subtitle"></div>
          </div>
        </div>
        <div id="topbar-actions"></div>
      </div>
      <div id="page-content" class="page"></div>
    </div>
  `;
  return layout;
}

window.toggleTheme = function() {
  applyTheme(state.theme === 'dark' ? 'light' : 'dark');
};
window.navigate = navigate;
window.logout   = logout;
window.copyText = copyText;
window.tryLogin = tryLogin;

window.toggleSidebar = function() {
  const sidebar  = document.getElementById('main-sidebar');
  const backdrop = document.getElementById('sidebar-backdrop');
  if (!sidebar) return;
  const isOpen = sidebar.classList.contains('mob-open');
  if (isOpen) {
    sidebar.classList.remove('mob-open');
    if (backdrop) backdrop.classList.remove('show');
  } else {
    sidebar.classList.add('mob-open');
    if (backdrop) backdrop.classList.add('show');
  }
};

window.closeSidebar = function() {
  const sidebar  = document.getElementById('main-sidebar');
  const backdrop = document.getElementById('sidebar-backdrop');
  if (sidebar)  sidebar.classList.remove('mob-open');
  if (backdrop) backdrop.classList.remove('show');
};

// ─── 页面渲染路由 ───────────────────────────────────────────
async function renderPage(page) {
  const container = $('page-content');
  if (!container) return;
  container.innerHTML = '<div style="padding:2rem;text-align:center"><span class="spinner"></span></div>';

  const titles = {
    'dashboard':      ['邮箱总览', '管理您的临时邮箱'],
    'inbox':          ['邮件列表', ''],
    'email-view':     ['邮件内容', ''],
    'domains-guide':  ['域名列表 & 添加指南', '查看可用域名并了解如何添加新域名'],
    'admin-accounts': ['账户管理', '创建和管理用户账户'],
    'admin-domains':  ['域名管理', '管理域名池'],
    'admin-settings': ['系统设置', ''],
    'apikey-show':    ['API Key', ''],
    'api-docs':       ['API 接口文档', '查看所有可用 API 及调用示例'],
  };
  const [t, s] = titles[page] || ['—', ''];
  const title = $('topbar-title'); if (title) title.textContent = t;
  const sub   = $('topbar-subtitle'); if (sub) sub.textContent = s;
  const actions = $('topbar-actions'); if (actions) actions.innerHTML = '';

  try {
    switch(page) {
      case 'dashboard':      await renderDashboard(container); break;
      case 'inbox':          await renderInbox(container); break;
      case 'email-view':     await renderEmailView(container); break;
      case 'domains-guide':  await renderDomainsGuide(container); break;
      case 'admin-accounts': await renderAdminAccounts(container); break;
      case 'admin-domains':  await renderAdminDomains(container); break;
      case 'admin-settings': await renderAdminSettings(container); break;
      case 'apikey-show':    renderApiKeyShow(container); break;
      case 'api-docs':       renderApiDocs(container); break;
      default: container.innerHTML = '<div class="page"><p>页面未找到</p></div>';
    }
  } catch(e) {
    container.innerHTML = `<div style="padding:2rem;color:var(--clr-danger)">加载失败：${escHtml(e.message)}</div>`;
  }
}

// ─── Dashboard ─────────────────────────────────────────────
async function renderDashboard(container) {
  const isAdmin = state.account?.is_admin;
  const [mailboxes, domains, statsData] = await Promise.all([
    api.listMailboxes(),
    api.domains(),
    api.stats().catch(() => null),
  ]);
  state.mailboxes = mailboxes || [];

  const actions = $('topbar-actions');
  if (actions) {
    actions.innerHTML = `
      <button class="btn btn-primary btn-sm" onclick="createMailbox()">+ 新建邮箱</button>
      <button class="btn btn-ghost btn-sm" onclick="navigate('apikey-show')" style="margin-left:0.4rem">⚿ 我的 API Key</button>
    `;
  }

  const boxes  = state.mailboxes;
  const st     = statsData || {};
  const activeDomains  = (domains||[]).filter(d => d.is_active).length;
  const pendingDomains = (domains||[]).filter(d => d.status === 'pending').length;

  const statCards = [
    { label: '我的邮箱', value: boxes.length,                   note: '当前有效' },
    { label: '可用域名', value: activeDomains,                  note: `共 ${(domains||[]).length} 个` },
    { label: '收到邮件', value: st.total_emails ?? '—',         note: '全平台累计' },
    { label: '邮箱总量', value: st.total_mailboxes ?? '—',      note: `活跃 ${st.active_mailboxes ?? '—'} 个` },
    ...(isAdmin ? [
      { label: '账户总数', value: st.total_accounts ?? '—',       note: '注册用户' },
      { label: '待验证域名', value: st.pending_domains ?? pendingDomains, note: pendingDomains > 0 ? '🔄 验证中' : '无' },
    ] : []),
  ];

  // 公告栏
  const announcement = (await api.publicSettings().catch(() => ({}))).announcement || '';

  container.innerHTML = `
    ${announcement ? `<div class="card" style="margin-bottom:1rem;background:var(--clr-primary,#4f6ef7);color:#fff;padding:0.7rem 1rem;font-size:0.84rem">
      📢 ${escHtml(announcement)}</div>` : ''}
    <div class="stat-grid" style="grid-template-columns:repeat(auto-fill,minmax(140px,1fr))">
      ${statCards.map(s => `
        <div class="stat-card">
          <div class="stat-label">${escHtml(s.label)}</div>
          <div class="stat-value">${typeof s.value === 'number' ? s.value.toLocaleString() : s.value}</div>
          <div class="stat-note">${escHtml(s.note)}</div>
        </div>
      `).join('')}
    </div>
    ${pendingDomains > 0 ? `
      <div class="card" style="margin-top:0.8rem;border-left:3px solid var(--clr-warn,#e6a817)">
        <div style="font-size:0.82rem">🔄 有 ${pendingDomains} 个域名正在 MX 验证中，通过后将自动加入域名池</div>
      </div>
    ` : ''}
    ${boxes.length === 0 ? `
      <div class="card" style="margin-top:0.8rem">
        <div class="empty-state">
          <span class="empty-icon">✉</span>
          <p>还没有邮箱，点击右上角"新建邮箱"创建第一个</p>
        </div>
      </div>
    ` : `
      <div class="mailbox-grid" id="mailbox-grid" style="margin-top:0.8rem">
        ${boxes.map(mb => buildMailboxCard(mb)).join('')}
      </div>
    `}
  `;
}

function buildMailboxCard(mb) {
  const expiresAt = mb.expires_at ? new Date(mb.expires_at) : null;
  const now = new Date();
  let expiryHtml = '';
  if (expiresAt) {
    const diffMs = expiresAt - now;
    if (diffMs <= 0) {
      expiryHtml = '<span style="color:var(--clr-danger);font-size:0.75rem">⏱ 已过期</span>';
    } else {
      const mins = Math.ceil(diffMs / 60000);
      const color = mins <= 5 ? 'var(--clr-danger)' : mins <= 15 ? 'var(--clr-warn,#e6a817)' : 'var(--text-muted)';
      expiryHtml = `<span style="color:${color};font-size:0.75rem">⏱ ${mins}分钟后删除</span>`;
    }
  }
  return `
    <div class="mailbox-card" onclick="openInbox('${mb.id}','${escHtml(mb.full_address)}')">
      <div class="mailbox-address">${escHtml(mb.full_address)}</div>
      <div class="mailbox-stats" style="display:flex;gap:0.7rem;align-items:center">
        <span>创建于 ${formatDate(mb.created_at)}</span>
        ${expiryHtml}
      </div>
      <div class="mailbox-actions">
        <button class="btn btn-ghost btn-sm" onclick="event.stopPropagation();openInbox('${mb.id}','${escHtml(mb.full_address)}')">📬 查看邮件</button>
        <button class="btn btn-ghost btn-sm" onclick="event.stopPropagation();copyText('${escHtml(mb.full_address)}')" title="复制地址">⎘</button>
        <button class="btn btn-danger btn-sm" onclick="event.stopPropagation();confirmDeleteMailbox('${mb.id}','${escHtml(mb.full_address)}')">✕</button>
      </div>
    </div>
  `;
}

window.openInbox = function(id, addr) {
  state.currentMailbox = { id, full_address: addr };
  navigate('inbox');
};

window.createMailbox = async function() {
  // 拉取活跃域名列表，构建选择弹窗
  let activeDomains = [];
  try {
    const all = await api.domains();
    activeDomains = (all || []).filter(d => d.is_active);
  } catch(e) { /* 获取失败时退化为随机域名 */ }

  const old = document.querySelector('.modal-overlay');
  if (old) old.remove();
  const overlay = el('div', 'modal-overlay');

  const domainOptions = activeDomains.map(d =>
    `<option value="${escHtml(d.domain)}">${escHtml(d.domain)}</option>`
  ).join('');

  overlay.innerHTML = `
    <div class="modal" style="max-width:420px">
      <div class="modal-title">+ 新建临时邮箱</div>
      <button class="modal-close" onclick="this.closest('.modal-overlay').remove()">✕</button>
      <div class="form-group" style="margin-top:0.8rem">
        <label class="form-label">本地部分（@ 之前）</label>
        <input class="form-input" id="mb-address" placeholder="留空则随机生成" autocomplete="off" />
        <div class="form-hint">只允许字母、数字、连字符、下划线</div>
      </div>
      <div class="form-group">
        <label class="form-label">域名</label>
        <select class="form-input" id="mb-domain">
          <option value="">随机选取</option>
          ${domainOptions}
        </select>
      </div>
      <div class="modal-actions">
        <button class="btn btn-ghost" onclick="this.closest('.modal-overlay').remove()">取消</button>
        <button class="btn btn-primary" id="mb-confirm-btn">创建</button>
      </div>
    </div>
  `;
  document.body.appendChild(overlay);
  overlay.addEventListener('click', e => { if (e.target === overlay) overlay.remove(); });

  // 回车确认
  overlay.querySelector('#mb-address').addEventListener('keydown', e => {
    if (e.key === 'Enter') overlay.querySelector('#mb-confirm-btn').click();
  });

  overlay.querySelector('#mb-confirm-btn').addEventListener('click', async () => {
    const btn     = overlay.querySelector('#mb-confirm-btn');
    const address = overlay.querySelector('#mb-address').value.trim();
    const domain  = overlay.querySelector('#mb-domain').value;
    btn.disabled  = true;
    btn.textContent = '创建中...';
    try {
      const body = {};
      if (address) body.address = address;
      if (domain)  body.domain  = domain;
      const mb = await api.createMailbox(body);
      overlay.remove();
      toast(`已创建：${mb.full_address}`, 'success');
      navigate('dashboard');
    } catch(e) {
      btn.disabled = false;
      btn.textContent = '创建';
      toast('创建失败：' + e.message, 'error');
    }
  });
};

window.confirmDeleteMailbox = function(id, addr) {
  showModal(`删除邮箱`, `<p>确定删除 <strong>${escHtml(addr)}</strong>？<br/><span style="font-size:0.8rem;color:var(--clr-danger)">所有邮件将被永久删除。</span></p>`,
    async () => {
      try {
        await api.deleteMailbox(id);
        toast('邮箱已删除', 'success');
        navigate('dashboard');
      } catch(e) { toast('删除失败: ' + e.message, 'error'); }
    }
  );
};

// ─── API Key 展示 ──────────────────────────────────────────
function renderApiKeyShow(container) {
  const key = state.apiKey || '—';
  container.innerHTML = `
    <div class="card" style="max-width:540px">
      <div class="card-header"><div class="card-title">⚿ 我的 API Key</div></div>
      <div class="card-body">
        <p style="font-size:0.84rem;color:var(--text-secondary);margin-bottom:1rem">
          API Key 用于认证所有 API 请求。请勿泄露。
        </p>
        <div class="form-label">当前 API Key</div>
        <div class="code-box" style="margin-bottom:1rem">
          <span style="filter:blur(4px);cursor:pointer" id="key-blur" onclick="this.style.filter='none'">${escHtml(key)}</span>
          <button class="copy-btn" onclick="copyText('${escHtml(key)}')" title="复制">⎘</button>
        </div>
        <p style="font-size:0.76rem;color:var(--text-muted)">点击 Key 可显示明文。保存后请妥善保管，丢失需联系管理员重置。</p>
        <div class="divider"></div>
        <div class="form-label">HTTP 请求示例</div>
        <div class="code-box" style="font-size:0.75rem">curl -H "Authorization: Bearer &lt;api_key&gt;" http://server:8080/api/mailboxes</div>
      </div>
    </div>
  `;
}

// ─── Inbox ────────────────────────────────────────────────
async function renderInbox(container) {
  const mb = state.currentMailbox;
  if (!mb) { navigate('dashboard'); return; }

  const title = $('topbar-title'); if (title) title.textContent = mb.full_address;
  const sub   = $('topbar-subtitle'); if (sub) sub.textContent = '邮件列表';
  const actions = $('topbar-actions');
  if (actions) {
    actions.innerHTML = `
      <button class="btn btn-ghost btn-sm" onclick="copyText('${escHtml(mb.full_address)}')">⎘ 复制地址</button>
      <button class="btn btn-primary btn-sm" onclick="refreshInbox()" style="margin-left:0.4rem">↻ 刷新</button>
      <button class="btn btn-ghost btn-sm" onclick="navigate('dashboard')" style="margin-left:0.4rem">← 返回</button>
    `;
  }

  const emails = await api.listEmails(mb.id);
  state.emails = emails || [];

  // 启动自动刷新（每 8 秒）
  clearInboxPoller();
  _inboxPollerTimer = setInterval(async () => {
    if (state.page !== 'inbox') { clearInboxPoller(); return; }
    try {
      const fresh = await api.listEmails(mb.id);
      if (!fresh) return;
      // 有新邮件才重新渲染，避免闪烁
      if (fresh.length !== (state.emails || []).length ||
          (fresh[0]?.id !== state.emails?.[0]?.id)) {
        state.emails = fresh;
        const c = $('page-content');
        if (c) renderInbox(c);
      }
    } catch(e) { /* 静默失败 */ }
  }, 8000);

  if (!state.emails.length) {
    container.innerHTML = `
      <div class="card">
        <div class="empty-state">
          <span class="empty-icon">📭</span>
          <p>暂无邮件</p>
          <p style="margin-top:0.5rem;font-size:0.8rem">向 <strong>${escHtml(mb.full_address)}</strong> 发送邮件后，邮件将显示在此处</p>
        </div>
      </div>
    `;
    return;
  }

  container.innerHTML = `
    <div class="card" style="padding:0">
      ${state.emails.map(e => buildEmailItem(mb.id, e)).join('')}
    </div>
  `;
}

function buildEmailItem(mbId, e) {
  const from = e.sender || e.from_addr || '(无发件人)';
  const initials = from.charAt(0).toUpperCase();
  const preview = (e.body_text || e.text_body || '').slice(0, 80).replace(/\n/g, ' ');
  return `
    <div class="email-item" onclick="openEmail('${mbId}','${e.id}')">
      <div class="email-avatar">${escHtml(initials)}</div>
      <div class="email-meta">
        <div class="email-from">${escHtml(from)}</div>
        <div class="email-subject">${escHtml(e.subject || '(无主题)')}</div>
        <div class="email-preview">${escHtml(preview)}</div>
      </div>
      <div>
        <div class="email-time">${timeAgo(e.received_at)}</div>
        <button class="btn btn-ghost btn-sm" style="margin-top:0.3rem" onclick="event.stopPropagation();deleteEmail('${mbId}','${e.id}')">✕</button>
      </div>
    </div>
  `;
}

window.openEmail = function(mbId, eid) {
  state.currentMailbox = state.currentMailbox || { id: mbId };
  state.currentEmailId = eid;
  navigate('email-view');
};

window.refreshInbox = function() {
  clearInboxPoller();
  renderPage('inbox');
};

window.deleteEmail = async function(mbId, eid) {
  try {
    await api.deleteEmail(mbId, eid);
    toast('邮件已删除', 'success');
    navigate('inbox');
  } catch(e) { toast('删除失败: ' + e.message, 'error'); }
};

// ─── Email View ────────────────────────────────────────────
async function renderEmailView(container) {
  const mb = state.currentMailbox;
  const eid = state.currentEmailId;
  if (!mb || !eid) { navigate('dashboard'); return; }

  const actions = $('topbar-actions');
  if (actions) {
    actions.innerHTML = `
      <button class="btn btn-ghost btn-sm" onclick="navigate('inbox')">← 返回列表</button>
      <button class="btn btn-danger btn-sm" onclick="deleteEmail('${mb.id}','${eid}');navigate('inbox')" style="margin-left:0.4rem">删除</button>
    `;
  }

  const e = await api.getEmail(mb.id, eid);
  const fromAddr = e.sender || e.from_addr || '—';
  const toAddr   = mb.full_address || state.currentMailbox?.full_address || '—';
  const htmlBody  = e.body_html || e.html_body || '';
  const textBody  = e.body_text || e.text_body || '';
  const title = $('topbar-title'); if (title) title.textContent = e.subject || '(无主题)';
  const sub   = $('topbar-subtitle'); if (sub) sub.textContent = `来自：${fromAddr}`;

  // 先渲染完整 HTML（含 iframe 占位），再向 iframe 写入内容
  container.innerHTML = `
    <div class="card" style="padding:0;max-width:860px">
      <div class="email-detail-header">
        <div class="email-subject-big">${escHtml(e.subject || '(无主题)')}</div>
        <div class="email-info-row">
          <span>发件人：<strong>${escHtml(fromAddr)}</strong></span>
          <span style="margin:0 0.3rem">·</span>
          <span>收件人：<strong>${escHtml(toAddr)}</strong></span>
          <span style="margin:0 0.3rem">·</span>
          <span>${formatDate(e.received_at)}</span>
        </div>
      </div>
      ${htmlBody
        ? `<iframe class="email-body-frame" id="email-frame" sandbox="allow-same-origin allow-popups"></iframe>`
        : `<div class="email-body-text" style="white-space:pre-wrap">${escHtml(textBody || '(邮件内容为空)')}</div>`
      }
    </div>
  `;

  // innerHTML 中的 <script> 不会执行；在 DOM 就绪后直接向 iframe 写内容
  if (htmlBody) {
    const frame = container.querySelector('#email-frame');
    if (frame) {
      frame.contentDocument.open();
      frame.contentDocument.write(htmlBody);
      frame.contentDocument.close();
      const setH = () => {
        try { frame.style.height = frame.contentDocument.body.scrollHeight + 20 + 'px'; } catch (_) {}
      };
      frame.addEventListener('load', setH);
      setTimeout(setH, 300);
    }
  }
}

// ─── 域名列表 & 指南 ─────────────────────────────────────────
async function renderDomainsGuide(container) {
  const actions = $('topbar-actions');
  if (actions) {
    actions.innerHTML = `<button class="btn btn-success btn-sm" onclick="showMXRegisterModal()">⚡ 提交域名自动验证</button>`;
  }

  const [domains, pub] = await Promise.all([
    api.domains(),
    api.publicSettings().catch(() => ({})),
  ]);
  const smtpIP  = pub.smtp_server_ip || '';
  const smtpHostname = pub.smtp_hostname || '';
  const ipLabel = smtpIP || '&lt;服务器 IP&gt;';
  const mxTarget = smtpHostname || '&lt;服务器邮件主机名&gt;';
  const needsARec = !smtpHostname;

  const pending = (domains||[]).filter(d => d.status === 'pending');
  const active  = (domains||[]).filter(d => d.status !== 'pending');

  const pendingHtml = pending.length > 0 ? `
    <div class="card" style="border-left:3px solid var(--clr-warn,#e6a817);margin-bottom:1rem">
      <div class="card-header">
        <div class="card-title">🔄 待 MX 验证 (${pending.length})</div>
        <div style="font-size:0.78rem;color:var(--text-muted)">后台每 30 秒自动检测，验证通过后自动激活</div>
      </div>
      <div class="table-wrap">
        <table>
          <thead><tr><th>域名</th><th>上次检测</th><th>状态</th></tr></thead>
          <tbody>
            ${pending.map(d => `
              <tr id="pending-row-${d.id}">
                <td style="font-family:var(--font-mono);font-size:0.82rem">${escHtml(d.domain)}</td>
                <td style="font-size:0.78rem">${d.mx_checked_at ? timeAgo(d.mx_checked_at) : '待首次检测'}</td>
                <td><span class="badge badge-gold" id="pending-status-${d.id}">⏳ 检测中</span></td>
              </tr>
            `).join('')}
          </tbody>
        </table>
      </div>
    </div>
  ` : '';

  container.innerHTML = `
    ${pendingHtml}
    <div class="domain-guide-grid" style="display:grid;grid-template-columns:1fr 1fr;gap:1.2rem;max-width:1000px">
      <div>
        <div class="card">
          <div class="card-header"><div class="card-title">◎ 可用域名池</div></div>
          <div class="table-wrap">
            <table>
              <thead><tr><th>域名</th><th>状态</th></tr></thead>
              <tbody>
                ${active.length === 0
                  ? `<tr><td colspan="2" style="text-align:center;color:var(--text-muted)">暂无域名</td></tr>`
                  : active.map(d => `
                    <tr>
                      <td style="font-family:var(--font-mono);font-size:0.82rem">${escHtml(d.domain)}</td>
                      <td>${d.is_active
                        ? '<span class="badge badge-green">● 启用</span>'
                        : '<span class="badge badge-gray">○ 停用</span>'}</td>
                    </tr>
                  `).join('')}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      <div>
        <div class="card">
          <div class="card-header"><div class="card-title">📖 添加域名指南</div></div>
          <div class="card-body">
            <div class="guide-step">
              <div class="step-num">1</div>
              <div class="step-body">
                <div class="step-title">准备域名</div>
                <div class="step-desc">在域名注册商处购买一个域名，例如 <code>example.com</code>，并获取 DNS 管理权限。</div>
              </div>
            </div>
            <div class="guide-step">
              <div class="step-num">2</div>
              <div class="step-body">
                <div class="step-title">配置 MX 记录（仅需一条）</div>
                <div class="step-desc">在 DNS 面板添加以下记录，让 SMTP 邮件投递到本服务器：</div>
                <table class="dns-table" style="margin-top:0.5rem">
                  <thead><tr><th>类型</th><th>主机名</th><th>内容</th><th>优先级</th></tr></thead>
                  <tbody>
                    <tr><td>MX</td><td>@</td><td style="font-family:monospace">${mxTarget}</td><td>10</td></tr>
                    ${needsARec ? `<tr><td>A</td><td style="font-family:monospace">mail.yourdomain.com</td><td style="font-family:monospace">${ipLabel}</td><td>—</td></tr>` : ''}
                    <tr><td>TXT</td><td>@</td><td style="font-family:monospace">v=spf1 ip4:${ipLabel} ~all</td><td>—</td></tr>
                  </tbody>
                </table>
              </div>
            </div>
            <div class="guide-step">
              <div class="step-num">3</div>
              <div class="step-body">
                <div class="step-title">提交域名自动验证</div>
                <div class="step-desc">
                  DNS 广播后（通常 5–30 分钟），点击右上角「⚡ 提交域名自动验证」按钮。<br>
                  <ul style="margin:0.4rem 0 0 1rem;font-size:0.82rem">
                    <li>MX 已生效 → <b>立即激活</b>加入域名池</li>
                    <li>MX 未生效 → 进入<b>待验证队列</b>，后台每 30 秒自动重试</li>
                  </ul>
                </div>
                <button class="btn btn-success btn-sm" style="margin-top:0.5rem" onclick="showMXRegisterModal()">⚡ 提交域名</button>
              </div>
            </div>
            <div class="guide-step">
              <div class="step-num">4</div>
              <div class="step-body">
                <div class="step-title">验证收信</div>
                <div class="step-desc">域名激活后，创建该域名下的邮箱，用其他邮件客户端发送测试邮件，30 秒内应能收到。</div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  `;

  if (pending.length > 0) {
    startPendingDomainPoller(pending.map(d => d.id));
  }
}

// ─── Admin: 账户管理 ─────────────────────────────────────────
async function renderAdminAccounts(container) {
  const actions = $('topbar-actions');
  if (actions) {
    actions.innerHTML = `<button class="btn btn-primary btn-sm" onclick="showCreateAccountModal()">+ 创建账户</button>`;
  }

  const accounts = await api.admin.listAccounts();
  container.innerHTML = `
    <div class="card" style="max-width:860px">
      <div class="card-header">
        <div class="card-title">👥 账户列表</div>
        <div style="font-size:0.78rem;color:var(--text-muted)">共 ${(accounts||[]).length} 个账户</div>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr><th>用户名</th><th>角色</th><th>创建时间</th><th>操作</th></tr>
          </thead>
          <tbody>
            ${(accounts||[]).map(a => `
              <tr>
                <td>
                  <div style="font-weight:600">${escHtml(a.username || '—')}</div>
                  <div class="code-box" style="margin-top:0.3rem;font-size:0.72rem">
                    <span>${escHtml(a.api_key || '—')}</span>
                    <button class="copy-btn" onclick="copyText('${escHtml(a.api_key||'')}')">⎘</button>
                  </div>
                </td>
                <td>${a.is_admin
                  ? '<span class="badge badge-gold">管理员</span>'
                  : '<span class="badge badge-gray">普通用户</span>'}</td>
                <td style="font-size:0.8rem">${formatDate(a.created_at)}</td>
                <td>
                  ${!a.is_admin ? `<button class="btn btn-danger btn-sm" onclick="confirmDeleteAccount('${a.id}','${escHtml(a.username||'')}')">删除</button>` : ''}
                </td>
              </tr>
            `).join('')}
          </tbody>
        </table>
      </div>
    </div>
  `;
}

window.showCreateAccountModal = function() {
  showModal('创建账户', `
    <div class="form-group">
      <label class="form-label">用户名</label>
      <input class="form-input" id="new-acc-username" placeholder="username" />
    </div>
    <div class="form-group">
      <label class="form-label">
        <input type="checkbox" id="new-acc-admin" style="margin-right:0.4rem">
        设为管理员
      </label>
    </div>
  `, async () => {
    const username = ($('new-acc-username')?.value || '').trim();
    if (!username) { toast('请输入用户名', 'warn'); return false; }
    const is_admin = $('new-acc-admin')?.checked || false;
    try {
      await api.admin.createAccount({ username, is_admin });
      toast('账户已创建', 'success');
      navigate('admin-accounts');
    } catch(e) { toast('创建失败: ' + e.message, 'error'); return false; }
  });
};

window.confirmDeleteAccount = function(id, name) {
  showModal('删除账户', `<p>确定删除账户 <strong>${escHtml(name)}</strong>？</p>`, async () => {
    try {
      await api.admin.deleteAccount(id);
      toast('账户已删除', 'success');
      navigate('admin-accounts');
    } catch(e) { toast('删除失败: ' + e.message, 'error'); }
  });
};

// ─── Admin: 域名管理 ─────────────────────────────────────────
async function renderAdminDomains(container) {
  const actions = $('topbar-actions');
  if (actions) {
    actions.innerHTML = `
      <button class="btn btn-primary btn-sm" onclick="showAddDomainModal()">+ 手动添加</button>
      <button class="btn btn-success btn-sm" onclick="showMXRegisterModal()" style="margin-left:0.4rem">⚡ MX 自动注册</button>
    `;
  }

  const domains = await api.domains();
  const pending  = (domains||[]).filter(d => d.status === 'pending');
  const active   = (domains||[]).filter(d => d.status !== 'pending');

  container.innerHTML = `
    <div style="max-width:760px;display:flex;flex-direction:column;gap:1rem">
      ${pending.length > 0 ? `
        <div class="card" style="border-left:3px solid var(--clr-warn,#e6a817)">
          <div class="card-header">
            <div class="card-title">🔄 待 MX 验证 (${pending.length})</div>
            <div style="font-size:0.78rem;color:var(--text-muted)">后台每 30 秒自动检测，验证通过后自动加入域名池</div>
          </div>
          <div class="table-wrap">
            <table>
              <thead><tr><th>域名</th><th>上次检测</th><th>操作</th></tr></thead>
              <tbody id="pending-domains-tbody">
                ${pending.map(d => `
                  <tr id="pending-row-${d.id}">
                    <td style="font-family:var(--font-mono)">${escHtml(d.domain)}</td>
                    <td style="font-size:0.78rem">${d.mx_checked_at ? timeAgo(d.mx_checked_at) : '从未'}</td>
                    <td>
                      <span class="badge badge-gold" id="pending-status-${d.id}">⏳ 检测中</span>
                      <button class="btn btn-danger btn-sm" style="margin-left:0.4rem" onclick="confirmDeleteDomain(${d.id},'${escHtml(d.domain)}')">✕</button>
                    </td>
                  </tr>
                `).join('')}
              </tbody>
            </table>
          </div>
        </div>
      ` : ''}

      <div class="card">
        <div class="card-header">
          <div class="card-title">🌐 域名列表</div>
          <div style="font-size:0.78rem;color:var(--text-muted)">共 ${active.length} 个</div>
        </div>
        <div class="table-wrap">
          <table>
            <thead><tr><th>域名</th><th>状态</th><th>操作</th></tr></thead>
            <tbody>
              ${active.length === 0 ? `<tr><td colspan="3" style="text-align:center;color:var(--text-muted)">暂无域名</td></tr>` :
                active.map(d => `
                  <tr>
                    <td style="font-family:var(--font-mono)">${escHtml(d.domain)}</td>
                    <td>${d.is_active
                      ? '<span class="badge badge-green">● 启用</span>'
                      : '<span class="badge badge-gray">○ 停用</span>'}</td>
                    <td style="display:flex;gap:0.5rem;align-items:center">
                      <button class="btn btn-ghost btn-sm" onclick="toggleDomain(${d.id},${!d.is_active})">${d.is_active ? '停用' : '启用'}</button>
                      <button class="btn btn-danger btn-sm" onclick="confirmDeleteDomain(${d.id},'${escHtml(d.domain)}')">删除</button>
                    </td>
                  </tr>
                `).join('')}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  `;

  // 如果有 pending 域名，开始轮询
  if (pending.length > 0) {
    startPendingDomainPoller(pending.map(d => d.id));
  }
}

window.showAddDomainModal = function() {
  const old = document.querySelector('.modal-overlay');
  if (old) old.remove();

  let serverIP = '';
  let serverHostname = '';
  api.publicSettings().then(s => {
    serverIP = s.smtp_server_ip || '';
    serverHostname = s.smtp_hostname || '';
    updateDnsHint();
  }).catch(() => {});

  const overlay = el('div', 'modal-overlay');
  overlay.innerHTML = `
    <div class="modal" style="max-width:580px">
      <div class="modal-title">添加域名</div>
      <button class="modal-close" onclick="this.closest('.modal-overlay').remove()">✕</button>

      <div id="add-step1">
        <div class="form-group" style="margin-bottom:0.5rem">
          <label class="form-label">域名</label>
          <input class="form-input" id="add-domain-inp" placeholder="example.com" autofocus />
          <div class="form-hint">输入将用于接收邮件的顶级域名</div>
        </div>
        <div id="add-dns-hint" style="background:var(--bg-secondary);border-radius:6px;padding:0.7rem 0.9rem;margin-bottom:0.8rem;font-size:0.8rem">
          <b>需要配置的 DNS 记录：</b>
          <table style="margin-top:0.5rem;width:100%;border-collapse:collapse;font-size:0.76rem">
            <thead><tr><th style="text-align:left;padding:2px 5px">类型</th><th style="text-align:left;padding:2px 5px">主机名</th><th style="text-align:left;padding:2px 5px">内容</th><th style="text-align:left;padding:2px 5px">优先级</th></tr></thead>
            <tbody id="add-dns-rows"></tbody>
          </table>
        </div>
        <div id="add-mx-result" style="display:none;margin-bottom:0.7rem"></div>
        <div class="modal-actions" id="add-actions">
          <button class="btn btn-ghost" onclick="this.closest('.modal-overlay').remove()">取消</button>
          <button class="btn btn-secondary" id="add-check-btn" onclick="doAddDomainCheck(false)">🔍 检测 MX</button>
          <button class="btn btn-primary"  id="add-force-btn" style="display:none" onclick="doAddDomainCheck(true)">⚡ 强制添加</button>
        </div>
      </div>

      <div id="add-step2" style="display:none"></div>
    </div>
  `;
  document.body.appendChild(overlay);
  overlay.addEventListener('click', e => { if (e.target === overlay) overlay.remove(); });

  const inp = overlay.querySelector('#add-domain-inp');
  inp?.addEventListener('keydown', e => { if (e.key === 'Enter') window.doAddDomainCheck(false); });
  inp?.addEventListener('input', updateDnsHint);

  function updateDnsHint() {
    const d = (inp?.value || '').trim() || 'example.com';
    const ip = serverIP || '&lt;服务器IP&gt;';
    const hn = serverHostname || 'mail.' + d;
    const hasHostname = !!serverHostname;
    const tbody = document.getElementById('add-dns-rows');
    if (!tbody) return;
    tbody.innerHTML = `
      <tr><td style="padding:2px 5px">MX</td><td style="padding:2px 5px;font-family:monospace">@</td><td style="padding:2px 5px;font-family:monospace">${escHtml(hn)}</td><td style="padding:2px 5px">10</td></tr>
      ${hasHostname ? '' : `<tr><td style="padding:2px 5px">A</td><td style="padding:2px 5px;font-family:monospace">mail.${escHtml(d)}</td><td style="padding:2px 5px;font-family:monospace">${escHtml(ip)}</td><td style="padding:2px 5px">—</td></tr>`}
      <tr><td style="padding:2px 5px">TXT</td><td style="padding:2px 5px;font-family:monospace">@</td><td style="padding:2px 5px;font-family:monospace">v=spf1 ip4:${escHtml(ip)} ~all</td><td style="padding:2px 5px">—</td></tr>
    `;
  }
  updateDnsHint();

  window.doAddDomainCheck = async function(force) {
    const domain = (inp?.value || '').trim().toLowerCase();
    if (!domain) { toast('请输入域名', 'warn'); return; }
    const checkBtn = document.getElementById('add-check-btn');
    const forceBtn = document.getElementById('add-force-btn');
    const resEl    = document.getElementById('add-mx-result');
    if (checkBtn) { checkBtn.disabled = true; checkBtn.textContent = '检测中...'; }

    try {
      if (force) {
        // 强制直接添加（跳过 MX 检测）
        const r = await api.admin.addDomain({ domain });
        showDnsInstructions(domain, r);
        overlay.remove();
        return;
      }

      // 先做 MX 检测（force:false）
      let r;
      try {
        r = await api.admin.mxImport({ domain, force: false });
        // MX 通过 → 已添加
        const step1 = document.getElementById('add-step1');
        const step2 = document.getElementById('add-step2');
        if (step1) step1.style.display = 'none';
        if (step2) {
          step2.style.display = 'block';
          step2.innerHTML = `
            <div style="text-align:center;padding:1.2rem 0">
              <div style="font-size:2rem">✅</div>
              <h3 style="margin:0.5rem 0">MX 验证通过</h3>
              <p style="font-size:0.84rem;color:var(--text-secondary)">域名 <strong>${escHtml(domain)}</strong> 已立即加入域名池</p>
              <button class="btn btn-primary" style="margin-top:1rem" onclick="this.closest('.modal-overlay').remove();navigate('admin-domains')">查看域名列表</button>
            </div>`;
        }
        toast('✓ ' + domain + ' MX 验证通过，已加入域名池', 'success');
      } catch(err) {
        // MX 未通过 → 提示强制添加选项
        if (checkBtn) { checkBtn.disabled = false; checkBtn.textContent = '🔍 检测 MX'; }
        if (forceBtn) forceBtn.style.display = '';
        if (resEl) {
          resEl.style.display = 'block';
          resEl.innerHTML = `
            <div style="background:var(--clr-warn-bg,#fff8e1);border:1px solid var(--clr-warn,#e6a817);border-radius:6px;padding:0.6rem 0.9rem;font-size:0.82rem">
              ⚠️ <b>MX 记录未检测到</b>：${escHtml(err.message)}<br>
              <span style="color:var(--text-muted)">请先配置上方 DNS 记录后重新检测，或点击「强制添加」跳过检测直接加入域名池</span>
            </div>`;
        }
      }
    } catch(e) {
      if (checkBtn) { checkBtn.disabled = false; checkBtn.textContent = '🔍 检测 MX'; }
      toast('操作失败: ' + e.message, 'error');
    }
  };
};

// \u5c55\u793a\u6dfb\u52a0\u57df\u540d\u540e\u7684 DNS \u914d\u7f6e\u6307\u5f15
function showDnsInstructions(domain, result) {
  const dns = result.dns_records || [];
  const rows = dns.map(r => `
    <tr>
      <td style="padding:3px 8px;font-weight:600">${escHtml(r.type)}</td>
      <td style="padding:3px 8px">${escHtml(r.host)}</td>
      <td style="padding:3px 8px;font-family:monospace;font-size:0.78rem">${escHtml(r.value)}</td>
      <td style="padding:3px 8px">${r.priority || '\u2014'}</td>
    </tr>`).join('');
  const old = document.querySelector('.modal-overlay');
  if (old) old.remove();
  const overlay = el('div', 'modal-overlay');
  overlay.innerHTML = `
    <div class="modal" style="max-width:600px">
      <div class="modal-title">\u2705 \u57df\u540d\u5df2\u6dfb\u52a0\uff1a${escHtml(domain)}</div>
      <p style="font-size:0.84rem;color:var(--text-secondary);margin:0.5rem 0 0.8rem">
        \u8bf7\u5728 DNS \u7ba1\u7406\u9762\u677f\u6dfb\u52a0\u4ee5\u4e0b\u8bb0\u5f55\uff0c\u4e00\u822c 5\u201330 \u5206\u949f\u751f\u6548\uff1a
      </p>
      <div class="table-wrap">
        <table>
          <thead><tr><th>\u7c7b\u578b</th><th>\u4e3b\u673a\u540d</th><th>\u5185\u5bb9</th><th>\u4f18\u5148\u7ea7</th></tr></thead>
          <tbody>${rows}</tbody>
        </table>
      </div>
      <p style="font-size:0.78rem;color:var(--text-muted);margin-top:0.6rem">\u2139\ufe0f ${escHtml(result.instructions || '')}</p>
      <div class="modal-actions">
        <button class="btn btn-primary" onclick="this.closest('.modal-overlay').remove();navigate('admin-domains')">
          \u5b8c\u6210\uff0c\u67e5\u770b\u57df\u540d\u5217\u8868
        </button>
      </div>
    </div>
  `;
  document.body.appendChild(overlay);
  overlay.addEventListener('click', e => { if (e.target === overlay) { overlay.remove(); navigate('admin-domains'); }});
}

window.toggleDomain = async function(id, newActive) {
  try {
    await api.admin.toggleDomain(id, newActive);
    toast('状态已切换', 'success');
    navigate('admin-domains');
  } catch(e) { toast('操作失败: ' + e.message, 'error'); }
};

window.confirmDeleteDomain = function(id, name) {
  showModal('删除域名', `<p>确定删除域名 <strong>${escHtml(name)}</strong>？</p>`, async () => {
    try {
      await api.admin.deleteDomain(id);
      toast('域名已删除', 'success');
      navigate('admin-domains');
    } catch(e) { toast('删除失败: ' + e.message, 'error'); }
  });
};

// ─── Admin: 系统设置 ─────────────────────────────────────────
async function renderAdminSettings(container) {
  let settings = {};
  try { settings = await api.admin.getSettings(); } catch {}

  const regOpen    = settings.registration_open === 'true' || settings.registration_open === true;
  const smtpIp      = settings.smtp_server_ip       || '';
  const smtpHostname = settings.smtp_hostname         || '';
  const siteTitle  = settings.site_title            || 'TempMail';
  const defDomain  = settings.default_domain        || '';
  const ttlMins    = settings.mailbox_ttl_minutes   || '30';
  const announce   = settings.announcement          || '';
  const maxMb      = settings.max_mailboxes_per_user|| '5';

  function inputRow(id, label, value, hint, placeholder = '', settingKey = '') {
    const key = settingKey || id.replace(/^input-/, '').replace(/-/g, '_');
    return `
      <div class="form-group">
        <label class="form-label">${label}</label>
        <div style="display:flex;gap:0.5rem">
          <input class="form-input" id="${id}" value="${escHtml(value)}" placeholder="${escHtml(placeholder)}" style="flex:1" />
          <button class="btn btn-primary btn-sm" onclick="saveSetting('${id}','${key}')">✓ 保存</button>
        </div>
        ${hint ? `<div class="form-hint">${hint}</div>` : ''}
      </div>`;
  }

  container.innerHTML = `
    <div class="card" style="max-width:640px">
      <div class="card-header"><div class="card-title">⚙ 系统设置</div></div>
      <div class="card-body" style="display:flex;flex-direction:column;gap:0.1rem">

        <!-- 注册开关 -->
        <div class="toggle-wrap" style="margin-bottom:0.5rem">
          <label class="toggle">
            <input type="checkbox" id="toggle-reg" ${regOpen ? 'checked' : ''} onchange="saveRegistrationSetting(this.checked)">
            <span class="toggle-slider"></span>
          </label>
          <div>
            <div class="toggle-label">开放自行注册</div>
            <span class="toggle-desc">开启后未登录用户可在登录页自行注册账户</span>
          </div>
        </div>
        <div class="divider"></div>

        <!-- 站点名称 -->
        ${inputRow('input-site-title', '站点名称', siteTitle, '显示在标题栏和登录页', 'TempMail')}
        <div class="divider"></div>

        <!-- 公告 -->
        <div class="form-group">
          <label class="form-label">公告内容</label>
          <div style="display:flex;gap:0.5rem">
            <textarea class="form-input" id="input-announcement" rows="2" placeholder="留空则不显示公告" style="flex:1;resize:vertical">${escHtml(announce)}</textarea>
            <button class="btn btn-primary btn-sm" onclick="saveSetting('input-announcement','announcement')" style="align-self:flex-start">✓ 保存</button>
          </div>
          <div class="form-hint">显示在已登录用户的 Dashboard 顶部</div>
        </div>
        <div class="divider"></div>

        <!-- SMTP IP -->
        ${inputRow('input-smtp-ip', 'SMTP 服务器公网 IP', smtpIp, '用于生成 SPF DNS 配置提示', '0.0.0.0', 'smtp_server_ip')}
        <div class="divider"></div>

        <!-- SMTP Hostname -->
        ${inputRow('input-smtp-hostname', '邮件服务器主机名', smtpHostname, '用作 MX 记录目标（如 mail.yourdomain.com）。设置后用户添加域名只需一条 MX 记录，无需额外 A 记录。', 'mail.yourdomain.com', 'smtp_hostname')}
        <div class="divider"></div>

        <!-- 默认邮箱域名 -->
        ${inputRow('input-default-domain', '默认邮箱域名', defDomain, '创建邮箱时下拉框优先选中的域名', 'mail.example.com')}
        <div class="divider"></div>

        <!-- 邮箱 TTL -->
        ${inputRow('input-mailbox-ttl-minutes', '邮箱有效期（分钟）', ttlMins, '新建邮箱的默认存活时间，0 = 永不过期', '30')}
        <div class="divider"></div>

        <!-- 每用户邮箱上限 -->
        ${inputRow('input-max-mailboxes-per-user', '每账户邮箱上限', maxMb, '每个账户同时存在的邮箱数量上限', '5')}
        <div class="divider"></div>

        <!-- 服务信息 -->
        <div style="font-size:0.82rem;color:var(--text-secondary)">
          <strong>服务信息</strong>
          <p style="margin-top:0.5rem;line-height:2">
            SMTP IP:&nbsp;<code>${escHtml(smtpIp||'<未设置>')}</code><br>
            邮件主机名:&nbsp;<code>${escHtml(smtpHostname||'<未设置>')}</code><br>
            API:&nbsp;<code>${window.location.origin}/api</code><br>
            前端:&nbsp;<code>${window.location.origin}</code>
          </p>
        </div>
        <div class="divider"></div>

        <!-- 管理员 Key -->
        <div>
          <div class="form-label">管理员 API Key</div>
          <div class="code-box" style="font-size:0.78rem">
            <span style="filter:blur(4px);cursor:pointer" onclick="this.style.filter='none'">${escHtml(state.apiKey)}</span>
            <button class="copy-btn" onclick="copyText('${escHtml(state.apiKey)}')">⎘</button>
          </div>
          <div class="form-hint">Key 文件位置：<code>/data/admin.key</code>（API 服务容器内）</div>
        </div>

      </div>
    </div>
  `;
}

// 通用保存
window.saveSetting = async function(inputId, settingKey) {
  const el2 = document.getElementById(inputId);
  const val = el2 ? (el2.tagName === 'TEXTAREA' ? el2.value : el2.value.trim()) : '';
  try {
    await api.admin.saveSettings({ [settingKey]: val });
    toast('已保存', 'success');
  } catch(e) { toast('保存失败: ' + e.message, 'error'); }
};

// 兼容旧调用
window.saveSmtpIp = async function() { await window.saveSetting('input-smtp-ip', 'smtp_server_ip'); };

window.saveRegistrationSetting = async function(enabled) {
  try {
    await api.admin.saveSettings({ registration_open: enabled ? 'true' : 'false' });
    toast(`注册已${enabled ? '开启' : '关闭'}`, 'success');
  } catch(e) {
    toast('保存失败: ' + e.message, 'error');
    const cb = $('toggle-reg');
    if (cb) cb.checked = !enabled;
  }
};

// ─── Modal ────────────────────────────────────────────────
function showModal(title, bodyHtml, onConfirm) {
  const old = document.querySelector('.modal-overlay');
  if (old) old.remove();

  const overlay = el('div', 'modal-overlay');
  overlay.innerHTML = `
    <div class="modal">
      <div class="modal-title">${escHtml(title)}</div>
      <button class="modal-close" onclick="this.closest('.modal-overlay').remove()">✕</button>
      ${bodyHtml}
      <div class="modal-actions">
        <button class="btn btn-ghost" onclick="this.closest('.modal-overlay').remove()">取消</button>
        <button class="btn btn-primary" id="modal-confirm-btn">确认</button>
      </div>
    </div>
  `;
  document.body.appendChild(overlay);
  overlay.addEventListener('click', e => { if (e.target === overlay) overlay.remove(); });

  const confirmBtn = overlay.querySelector('#modal-confirm-btn');
  confirmBtn.addEventListener('click', async () => {
    confirmBtn.disabled = true;
    const result = await onConfirm();
    if (result !== false) overlay.remove();
    else confirmBtn.disabled = false;
  });
}

// ─── MX 自动注册（全自动验证流程）──────────────────────────
// 轮询待验证域名状态
let _pendingPollerTimer = null;
let _inboxPollerTimer   = null;
function clearInboxPoller() {
  if (_inboxPollerTimer) { clearInterval(_inboxPollerTimer); _inboxPollerTimer = null; }
}
function startPendingDomainPoller(ids) {
  if (!ids || ids.length === 0) return;
  clearInterval(_pendingPollerTimer);
  const remaining = new Set(ids);
  _pendingPollerTimer = setInterval(async () => {
    for (const id of [...remaining]) {
      try {
        const d = await api.getDomainStatus(id); // 使用非管理员接口
        const statusEl = document.getElementById('pending-status-' + id);
        const rowEl    = document.getElementById('pending-row-'   + id);
        if (d.status === 'active') {
          if (statusEl) statusEl.innerHTML = '<span class="badge badge-green">✓ 已激活</span>';
          remaining.delete(id);
          toast(`✓ 域名 ${d.domain} MX验证通过，已加入域名池`, 'success');
          setTimeout(() => { if (rowEl) rowEl.remove(); }, 3000);
        } else if (statusEl) {
          const ago = d.mx_checked_at ? timeAgo(d.mx_checked_at) : '从未';
          statusEl.innerHTML = `<span class="badge badge-gold">⏳ 检测中（上次${ago}）</span>`;
        }
      } catch {}
    }
    if (remaining.size === 0) clearInterval(_pendingPollerTimer);
  }, 5000);
}

window.showMXRegisterModal = function() {
  const old = document.querySelector('.modal-overlay');
  if (old) old.remove();
  const overlay = el('div', 'modal-overlay');
  overlay.innerHTML = `
    <div class="modal" style="max-width:560px">
      <div class="modal-title">⚡ MX 自动注册域名</div>
      <button class="modal-close" onclick="this.closest('.modal-overlay').remove()">✕</button>
      <p style="font-size:0.82rem;color:var(--text-secondary);margin:0.5rem 0 0.8rem">
        提交域名后系统立即检测 MX 记录。若已配置则直接激活；
        否则进入待验证队列，后台每 <b>30 秒</b>自动重试，无需手动确认。
      </p>
      <div class="form-group">
        <label class="form-label">域名（如 example.com）</label>
        <input class="form-input" id="mxr-domain" placeholder="example.com" autofocus />
      </div>
      <div id="mxr-dns-hint" style="display:none;background:var(--bg-secondary);border-radius:6px;padding:0.7rem 0.9rem;margin-bottom:0.6rem;font-size:0.8rem">
        <b>请在 DNS 管理面板添加以下记录：</b>
        <table style="margin-top:0.5rem;width:100%;border-collapse:collapse;font-size:0.76rem">
          <thead><tr><th style="text-align:left">类型</th><th style="text-align:left">主机名</th><th style="text-align:left">内容</th><th style="text-align:left">优先级</th></tr></thead>
          <tbody id="mxr-dns-rows"></tbody>
        </table>
      </div>
      <div id="mxr-status" style="display:none;margin-bottom:0.7rem"></div>
      <div class="modal-actions" id="mxr-actions">
        <button class="btn btn-ghost" onclick="this.closest('.modal-overlay').remove()">取消</button>
        <button class="btn btn-primary" id="mxr-submit">提交检测</button>
      </div>
    </div>
  `;
  document.body.appendChild(overlay);
  overlay.addEventListener('click', e => { if (e.target === overlay) overlay.remove(); });

  // 实时更新 DNS 提示
  const inp = overlay.querySelector('#mxr-domain');
  inp?.addEventListener('keydown', e => { if (e.key === 'Enter') submitMXRegister(); });

  overlay.querySelector('#mxr-submit').addEventListener('click', submitMXRegister);

  async function submitMXRegister() {
    const domain = (inp?.value || '').trim().toLowerCase();
    if (!domain) { toast('请输入域名', 'warn'); return; }
    const btn    = overlay.querySelector('#mxr-submit');
    const status = overlay.querySelector('#mxr-status');
    const hint   = overlay.querySelector('#mxr-dns-hint');
    btn.disabled = true;
    btn.textContent = '检测中...';
    status.style.display = 'none';

    const domainListPage = state.account?.is_admin ? 'admin-domains' : 'domains-guide';
    try {
      const r = await api.submitDomain({ domain }); // 任意已登录用户可用
      if (r.status === 'active') {
        overlay.innerHTML = `
          <div class="modal" style="text-align:center;padding:2rem">
            <div style="font-size:2rem">✅</div>
            <h3 style="margin:0.5rem 0">MX 验证通过</h3>
            <p style="font-size:0.84rem;color:var(--text-secondary)">域名 <strong>${escHtml(domain)}</strong> 已立即加入域名池</p>
            <button class="btn btn-primary" style="margin-top:1rem" onclick="this.closest('.modal-overlay').remove();navigate('${domainListPage}')">查看域名列表</button>
          </div>
        `;
        toast(`✓ ${domain} 已激活`, 'success');
      } else {
        // pending — 显示 DNS 配置 + 等待提示
        const rows = (r.dns_required || []).map(rec =>
          `<tr><td>${escHtml(rec.type)}</td><td style="font-family:monospace">${escHtml(rec.host)}</td><td style="font-family:monospace">${escHtml(rec.value)}</td><td>${rec.priority || '—'}</td></tr>`
        ).join('');
        overlay.querySelector('#mxr-dns-rows').innerHTML = rows;
        hint.style.display = 'block';

        status.style.display = 'block';
        status.innerHTML = `
          <div style="background:var(--clr-warn-bg,#fff8e1);border:1px solid var(--clr-warn,#e6a817);border-radius:6px;padding:0.6rem 0.9rem;font-size:0.81rem">
            ⏳ <b>域名已加入验证队列（ID ${r.domain.id}）</b><br>
            MX 记录配置生效后（通常 5-30 分钟），系统将自动激活。<br>
            <span style="color:var(--text-muted)">此窗口关闭后可在「域名列表」页查看验证进度</span>
          </div>
        `;
        const actionsEl = overlay.querySelector('#mxr-actions');
        actionsEl.innerHTML = `<button class="btn btn-primary" onclick="this.closest('.modal-overlay').remove();navigate('${domainListPage}')">前往域名列表查看进度</button>`;

        // 开始在当前 overlay 内轮询
        startInlinePoller(r.domain.id, domain, overlay);
      }
    } catch(e) {
      btn.disabled = false;
      btn.textContent = '重新提交';
      status.style.display = 'block';
      status.innerHTML = `<div style="color:var(--clr-danger);font-size:0.82rem">❌ ${escHtml(e.message)}</div>`;
    }
  }

  async function startInlinePoller(domainId, domainName, modal) {
    const statusEl = modal.querySelector('#mxr-status');
    let attempts = 0;
    const timer = setInterval(async () => {
      attempts++;
      if (!document.body.contains(modal)) { clearInterval(timer); return; }
      try {
        const d = await api.getDomainStatus(domainId); // 非管理员接口
        if (d.status === 'active') {
          clearInterval(timer);
          if (statusEl) statusEl.innerHTML = `
            <div style="background:#e8f5e9;border:1px solid #4caf50;border-radius:6px;padding:0.6rem 0.9rem;font-size:0.81rem">
              ✅ <b>MX 验证通过！域名 ${escHtml(domainName)} 已自动激活。</b>
            </div>`;
          toast(`✓ ${domainName} 已自动激活`, 'success');
          setTimeout(() => { modal.remove(); navigate(state.account?.is_admin ? 'admin-domains' : 'domains-guide'); }, 2500);
        } else if (statusEl) {
          const ago = d.mx_checked_at ? timeAgo(d.mx_checked_at) : '从未';
          statusEl.innerHTML = `
            <div style="background:var(--clr-warn-bg,#fff8e1);border:1px solid var(--clr-warn,#e6a817);border-radius:6px;padding:0.6rem 0.9rem;font-size:0.81rem">
              ⏳ 等待中（第 ${attempts} 次检测，上次 ${ago}）…
            </div>`;
        }
      } catch {}
    }, 5000);
  }
};

// ─── API 文档 ─────────────────────────────────────────
function renderApiDocs(container) {
  const key = state.apiKey || 'YOUR_API_KEY';
  const base = window.location.origin;
  const sections = [
    {
      title: '🔐 认证方式',
      desc: '所有 /api/* 接口需要在 HTTP Header 中携带 API Key：',
      code: `# Bearer Token 方式
curl -H "Authorization: Bearer ${key}" ${base}/api/me

# Query 参数方式
curl "${base}/api/me?api_key=${key}"`,
    },
    {
      title: '📫 1. 创建临时邮箱',
      desc: 'POST /api/mailboxes — address 和 domain 均为可选字段',
      code: `# 随机地址 + 随机域名
curl -s -X POST ${base}/api/mailboxes \\
  -H "Authorization: Bearer ${key}" \\
  -H "Content-Type: application/json" \\
  -d '{}'

# 指定本地部分（@ 之前），域名随机
curl -s -X POST ${base}/api/mailboxes \\
  -H "Authorization: Bearer ${key}" \\
  -H "Content-Type: application/json" \\
  -d '{"address": "mytestbox"}'

# 指定域名，地址随机（domain 须是已激活域名）
curl -s -X POST ${base}/api/mailboxes \\
  -H "Authorization: Bearer ${key}" \\
  -H "Content-Type: application/json" \\
  -d '{"domain": "example.com"}'

# 同时指定地址和域名
curl -s -X POST ${base}/api/mailboxes \\
  -H "Authorization: Bearer ${key}" \\
  -H "Content-Type: application/json" \\
  -d '{"address": "mytestbox", "domain": "example.com"}'

# 错误码：
#   400 → domain 不存在或未激活
#   409 → 地址已被占用（换一个 address 或留空让系统随机生成）
#   503 → 系统内无可用域名`,
    },
    {
      title: '📌 2. 获取邮箱列表',
      desc: 'GET /api/mailboxes — 获取当前账号下所有邮箱',
      code: `curl -s ${base}/api/mailboxes \\
  -H "Authorization: Bearer ${key}"

# 分页
 curl -s "${base}/api/mailboxes?page=1&size=20" \\
  -H "Authorization: Bearer ${key}"`,
    },
    {
      title: '📥 3. 获取邮箱收件箱（邮件列表）',
      desc: 'GET /api/mailboxes/:id/emails — 按收件时间倒序列出邮件摘要',
      code: `MAILBOX_ID="你的邮箱UUID"
curl -s ${base}/api/mailboxes/$MAILBOX_ID/emails \\
  -H "Authorization: Bearer ${key}"

# 分页
curl -s "${base}/api/mailboxes/$MAILBOX_ID/emails?page=1&size=20" \\
  -H "Authorization: Bearer ${key}"`,
    },
    {
      title: '📝 4. 读取单封邮件',
      desc: 'GET /api/mailboxes/:id/emails/:email_id — 获取邮件完整内容（含 HTML/纯文本和原始数据）',
      code: `MAILBOX_ID="你的邮箱UUID"
EMAIL_ID="你的邮件UUID"
curl -s ${base}/api/mailboxes/$MAILBOX_ID/emails/$EMAIL_ID \\
  -H "Authorization: Bearer ${key}"`,
    },
    {
      title: '🗑 5. 删除邮箱',
      desc: 'DELETE /api/mailboxes/:id — 立即删除邮箱及其所有邮件',
      code: `MAILBOX_ID="你的邮箱UUID"
curl -s -X DELETE ${base}/api/mailboxes/$MAILBOX_ID \\
  -H "Authorization: Bearer ${key}"`,
    },
    {
      title: '🗑 6. 删除单封邮件',
      desc: 'DELETE /api/mailboxes/:id/emails/:email_id',
      code: `curl -s -X DELETE ${base}/api/mailboxes/$MAILBOX_ID/emails/$EMAIL_ID \\
  -H "Authorization: Bearer ${key}"`,
    },
    {
      title: '🧪 7. 完整自动化示例（Shell 脚本）',
      desc: '创建邮箱 → 等待 5 秒 → 读取邮件 → 清理',
      code: `#!/bin/bash
BASE="${base}"
KEY="${key}"

# 1. 创建临时邮箱
MB=$(curl -s -X POST $BASE/api/mailboxes \\
  -H "Authorization: Bearer $KEY" \\
  -H "Content-Type: application/json" \\
  -d '{}')
MB_ID=$(echo $MB | python3 -c "import sys,json; print(json.load(sys.stdin)['mailbox']['id'])")
MB_ADDR=$(echo $MB | python3 -c "import sys,json; print(json.load(sys.stdin)['mailbox']['full_address'])")
echo "✓ 邮箱: $MB_ADDR (主键: $MB_ID)"

# 2. 向邮箱发送邮件...
echo "将测试邮件发到: $MB_ADDR"
sleep 5

# 3. 查看收件筱
EMAILS=$(curl -s $BASE/api/mailboxes/$MB_ID/emails \\
  -H "Authorization: Bearer $KEY")
echo "取到邮件: $EMAILS" | python3 -m json.tool

# 4. 读取第一封邮件（收件箱）
EMAIL_ID=$(echo $EMAILS | python3 -c "import sys,json;d=json.load(sys.stdin);print(d['data'][0]['id']) if d.get('data') else print('')" 2>/dev/null)
if [ -n "$EMAIL_ID" ]; then
  curl -s $BASE/api/mailboxes/$MB_ID/emails/$EMAIL_ID \\
    -H "Authorization: Bearer $KEY" | python3 -m json.tool
fi

# 5. 删除邮箱
curl -s -X DELETE $BASE/api/mailboxes/$MB_ID \\
  -H "Authorization: Bearer $KEY"
echo "✓ 邮箱已删除"`,
    },
    {
      title: '📈 8. 并发压测示例（wrk）',
      desc: '对注册接口进行高并发压测，500 并发，持续 30 秒',
      code: `# 安装 wrk: apt install wrk

# 导出注册脚本
cat > /tmp/register.lua << 'EOF'
wrk.method = "POST"
wrk.body   = '{"username": "user_' .. math.random(100000,999999) .. '"}'
wrk.headers["Content-Type"] = "application/json"
EOF

# 运行压测
wrk -t 10 -c 500 -d 30s --script /tmp/register.lua \\
  ${base}/public/register

# 或使用 k6
cat > /tmp/test.js << 'EOF'
import http from 'k6/http';
import { check } from 'k6';
export const options = { vus: 500, duration: '30s' };
const KEY = '${key}';
export default function() {
  const r = http.post(
    '${base}/api/mailboxes',
    '{}',
    { headers: { 'Authorization': 'Bearer ' + KEY, 'Content-Type': 'application/json' }}
  );
  check(r, { '创建成功': r => r.status === 201 });
}
EOF
k6 run /tmp/test.js`,
    },
  ];

  container.innerHTML = `
    <div style="max-width:860px">
      <div style="margin-bottom:1.2rem;padding:0.8rem 1rem;background:var(--bg-secondary);border-radius:8px;font-size:0.82rem">
        🔑 当前 API Key：
        <code style="margin-left:0.5rem;filter:blur(3px);cursor:pointer" onclick="this.style.filter='none'">${escHtml(key)}</code>
        <button class="copy-btn" onclick="copyText('${escHtml(key)}')" title="复制">⎘</button>
      </div>
      ${sections.map((s,i) => `
        <div class="card" style="margin-bottom:1rem">
          <div class="card-header"><div class="card-title">${escHtml(s.title)}</div></div>
          <div class="card-body">
            <p style="font-size:0.82rem;color:var(--text-secondary);margin-bottom:0.6rem">${escHtml(s.desc)}</p>
            <div class="code-box" style="white-space:pre;overflow-x:auto;font-size:0.75rem;line-height:1.6;position:relative">
              <button class="copy-btn" style="position:absolute;top:6px;right:6px" onclick="copyText(${JSON.stringify(s.code)})" title="复制">⎘</button>
              ${escHtml(s.code)}
            </div>
          </div>
        </div>
      `).join('')}
    </div>
  `;
}

// ─── 启动 ──────────────────────────────────────────────────
function init() {
  applyTheme(state.theme);

  if (state.apiKey && state.account) {
    showMainLayout();
    navigate('dashboard');
  } else if (state.apiKey) {
    // 验证 key
    tryLogin(state.apiKey);
  } else {
    showAuthPage();
  }
}

document.addEventListener('DOMContentLoaded', init);
