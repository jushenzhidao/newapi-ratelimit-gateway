/* NewAPI Rate Limit Gateway - Admin Web UI */
"use strict";

/* ================= 状态 ================= */
const STORE_KEY = "rl-gw-admin";
let state = loadState();

function loadState() {
  try {
    const raw = localStorage.getItem(STORE_KEY);
    if (raw) {
      const s = JSON.parse(raw);
      if (s.base && s.token) return { base: s.base, token: s.token, theme: s.theme || "dark" };
    }
  } catch (e) { /* ignore */ }
  return { base: "", token: "", theme: "dark" };
}

function saveState() {
  localStorage.setItem(STORE_KEY, JSON.stringify(state));
}

function clearState() {
  localStorage.removeItem(STORE_KEY);
}

/* ================= DOM 快捷引用 ================= */
const $ = (id) => document.getElementById(id);

const views = { login: $("login-view"), app: $("app-view") };
const modal = $("modal");
const keyModal = $("key-modal");
const confirmMask = $("confirm-mask");

/* ================= Toast ================= */
const TOAST_ICONS = {
  ok: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"><path d="M20 6L9 17l-5-5"/></svg>',
  err: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round"><path d="M18 6L6 18M6 6l12 12"/></svg>',
  warn: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><path d="M12 9v4M12 17h.01"/></svg>',
  info: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round"><circle cx="12" cy="12" r="9"/><path d="M12 8h.01M12 11v5"/></svg>',
};

function toast(msg, type = "info", ms = 3200) {
  const wrap = $("toast-wrap");
  const el = document.createElement("div");
  el.className = `toast ${type}`;
  el.innerHTML = `<span class="toast-ico">${TOAST_ICONS[type] || TOAST_ICONS.info}</span><span>${msg}</span>`;
  wrap.appendChild(el);
  setTimeout(() => {
    el.classList.add("out");
    setTimeout(() => el.remove(), 260);
  }, ms);
}

/* ================= API 封装 ================= */
async function api(path, options = {}) {
  const headers = { "Content-Type": "application/json", ...(options.headers || {}) };
  if (state.token) headers["Authorization"] = `Bearer ${state.token}`;
  let res;
  try {
    res = await fetch(state.base + path, { ...options, headers });
  } catch (e) {
    throw new Error(`无法连接网关：${state.base}（${e.message}）`);
  }
  if (res.status === 401) {
    // Token 失效：清会话并退回登录
    clearState();
    showLoginView("登录已过期，请重新登录");
    throw new Error("Unauthorized");
  }
  const body = await res.json().catch(() => ({}));
  if (!res.ok) {
    const detail = body.detail || body.message || body.error || `HTTP ${res.status}`;
    throw new Error(detail);
  }
  return body;
}

/* ================= 视图切换 ================= */
function showLoginView(msg) {
  views.login.classList.remove("hidden");
  views.app.classList.add("hidden");
  if (msg) toast(msg, "warn");
  if (state.base) $("login-base").value = state.base;
  if (state.token) $("login-token").value = state.token;
}

function showAppView() {
  views.login.classList.add("hidden");
  views.app.classList.remove("hidden");
  $("topbar-base").textContent = state.base;
  loadGroups();
}

/* ================= 登录 ================= */
async function doLogin(e) {
  e.preventDefault();
  const base = $("login-base").value.trim().replace(/\/+$/, "");
  const token = $("login-token").value.trim();
  if (!base || !token) { toast("请填写网关地址和管理 Token", "warn"); return; }

  const btn = $("login-btn");
  btn.disabled = true;
  btn.querySelector(".spinner").classList.remove("hidden");
  $("login-btn-text").textContent = "验证中…";

  // 用 GET /admin/groups 鉴权：能成功返回即鉴权通过
  const probe = { base, token };
  try {
    const res = await fetch(`${base}/admin/groups`, {
      headers: { Authorization: `Bearer ${token}` },
    });
    if (res.status === 200) {
      state.base = base;
      state.token = token;
      saveState();
      showAppView();
      toast("登录成功", "ok");
    } else if (res.status === 401) {
      toast("Token 无效，请检查 ADMIN_AUTH_TOKEN", "err");
    } else if (res.status === 403) {
      toast("管理接口未启用（ADMIN_ENABLED=false）", "err");
    } else {
      toast(`鉴权失败：HTTP ${res.status}`, "err");
    }
  } catch (err) {
    toast(`无法连接网关：${err.message}`, "err");
  } finally {
    btn.disabled = false;
    btn.querySelector(".spinner").classList.add("hidden");
    $("login-btn-text").textContent = "登 录";
  }
}

/* ================= 分组列表 ================= */
async function loadGroups() {
  const tbody = $("group-tbody");
  $("table-loading").classList.remove("hidden");
  $("table-empty").classList.add("hidden");
  tbody.innerHTML = "";
  try {
    const groups = await api("/admin/groups");
    $("group-count").textContent = groups.length;
    $("table-loading").classList.add("hidden");
    if (!groups.length) {
      $("table-empty").classList.remove("hidden");
      return;
    }
    tbody.innerHTML = groups.map(renderGroupRow).join("");
  } catch (err) {
    $("table-loading").classList.add("hidden");
    if (err.message !== "Unauthorized") {
      $("table-empty").classList.remove("hidden");
      $("table-empty").querySelector("p").textContent = "加载失败";
      $("table-empty").querySelector("span").textContent = err.message;
      toast(err.message, "err");
    }
  }
}

function esc(s) {
  return String(s == null ? "" : s).replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  }[c]));
}

function renderGroupRow(g) {
  const enabled = g.status === 1;
  return `
  <tr class="${enabled ? "" : "disabled-row"}">
    <td><span class="group-name">${esc(g.group_name)}</span></td>
    <td class="num"><span class="quota">${g.limit_5h}<small>次</small></span></td>
    <td class="num"><span class="quota">${g.limit_7d}<small>次</small></span></td>
    <td class="num"><span class="quota">${g.limit_30d}<small>次</small></span></td>
    <td><span class="badge badge-type">${esc(g.limit_type)}</span></td>
    <td><span class="badge badge-scope">${esc(g.scope)}</span></td>
    <td><span class="badge ${enabled ? "badge-ok" : "badge-off"}">${enabled ? "启用" : "已禁用"}</span></td>
    <td><span class="remark" title="${esc(g.remark || "")}">${esc(g.remark || "—")}</span></td>
    <td><span class="time">${esc(g.updated_at || g.created_at || "")}</span></td>
    <td class="ops">
      <button class="op-btn" data-act="edit" data-name="${esc(g.group_name)}" data-json='${esc(JSON.stringify(g))}'>编辑</button>
      ${enabled
        ? `<button class="op-btn danger" data-act="disable" data-name="${esc(g.group_name)}">禁用</button>`
        : `<button class="op-btn" data-act="enable" data-name="${esc(g.group_name)}">启用</button>`}
    </td>
  </tr>`;
}

$("group-tbody").addEventListener("click", (e) => {
  const btn = e.target.closest(".op-btn");
  if (!btn) return;
  const { act, name } = btn.dataset;
  if (act === "edit") {
    const g = JSON.parse(btn.dataset.json);
    openGroupModal(g);
  } else if (act === "disable") {
    confirmAction(`确定要<b>禁用</b>分组 <b>${esc(name)}</b> 吗？<br/>禁用后该分组限速立即失效，请求将不受限速。`, () => setGroupStatus(name, 0));
  } else if (act === "enable") {
    confirmAction(`确定要<b>启用</b>分组 <b>${esc(name)}</b> 吗？`, () => setGroupStatus(name, 1));
  }
});

async function setGroupStatus(name, status) {
  try {
    await api(`/admin/groups/${encodeURIComponent(name)}`, {
      method: "PUT",
      body: JSON.stringify({ status }),
    });
    toast(status === 1 ? `已启用 ${name}` : `已禁用 ${name}`, "ok");
    loadGroups();
  } catch (err) {
    if (err.message !== "Unauthorized") toast(err.message, "err");
  }
}

/* ================= 新建 / 编辑 ================= */
function openGroupModal(group) {
  $("modal-title").textContent = group ? "编辑分组" : "新建分组";
  $("f-orig-name").value = group ? group.group_name : "";
  $("f-name").value = group ? group.group_name : "";
  $("f-name").readOnly = !!group;
  $("f-name-hint").textContent = group ? "（名称不可修改）" : "";
  $("f-limit-5h").value = group ? group.limit_5h : "";
  $("f-limit-7d").value = group ? group.limit_7d : "";
  $("f-limit-30d").value = group ? group.limit_30d : "";
  $("f-type").value = group ? group.limit_type : "request";
  $("f-scope").value = group ? group.scope : "key";
  $("f-status").value = group ? String(group.status) : "1";
  $("f-remark").value = group ? (group.remark || "") : "";
  modal.classList.remove("hidden");
  setTimeout(() => $("f-name").focus(), 60);
}

async function submitGroup(e) {
  e.preventDefault();
  const orig = $("f-orig-name").value;
  const payload = {
    limit_5h: parseInt($("f-limit-5h").value, 10),
    limit_7d: parseInt($("f-limit-7d").value, 10),
    limit_30d: parseInt($("f-limit-30d").value, 10),
    limit_type: $("f-type").value,
    scope: $("f-scope").value,
    status: parseInt($("f-status").value, 10),
    remark: $("f-remark").value.trim(),
  };
  for (const k of ["limit_5h", "limit_7d", "limit_30d"]) {
    if (!Number.isInteger(payload[k]) || payload[k] < 1) {
      toast("配额必须是 ≥1 的整数", "warn");
      return;
    }
  }
  const btn = $("modal-save");
  btn.disabled = true;
  btn.querySelector(".spinner").classList.remove("hidden");
  try {
    if (orig) {
      const name = $("f-name").value.trim();
      await api(`/admin/groups/${encodeURIComponent(name)}`, { method: "PUT", body: JSON.stringify(payload) });
      toast("分组已更新", "ok");
    } else {
      const name = $("f-name").value.trim();
      if (!name) { toast("请填写分组名称", "warn"); return; }
      await api("/admin/groups", { method: "POST", body: JSON.stringify({ ...payload, group_name: name }) });
      toast("分组已创建", "ok");
    }
    modal.classList.add("hidden");
    loadGroups();
  } catch (err) {
    if (err.message !== "Unauthorized") toast(err.message, "err");
  } finally {
    btn.disabled = false;
    btn.querySelector(".spinner").classList.add("hidden");
  }
}

/* ================= 手动同步 ================= */
async function doSync() {
  const btn = $("btn-sync");
  btn.disabled = true;
  btn.querySelector("svg").style.animation = "spin 0.8s linear infinite";
  try {
    await api("/admin/sync", { method: "POST" });
    toast("同步已触发：Key→分组映射 / 分组配置已从数据库刷新", "ok");
  } catch (err) {
    if (err.message !== "Unauthorized") toast(err.message, "err");
  } finally {
    btn.disabled = false;
    btn.querySelector("svg").style.animation = "";
  }
}

/* ================= Key 状态查询 ================= */
async function queryKey(e) {
  e.preventDefault();
  const key = $("f-key").value.trim();
  if (!key) { toast("请输入 API Key", "warn"); return; }
  const btn = $("key-btn");
  btn.disabled = true;
  btn.querySelector(".spinner").classList.remove("hidden");
  const box = $("key-result");
  box.classList.remove("hidden");
  box.innerHTML = `<div class="loading-row"><span class="spinner dark"></span><em>查询中…</em></div>`;
  try {
    const res = await fetch(`${state.base}/ratelimit/status/${encodeURIComponent(key)}`);
    const data = await res.json().catch(() => ({}));
    box.innerHTML = renderKeyResult(data);
  } catch (err) {
    box.innerHTML = `<div class="key-error">查询失败：${esc(err.message)}</div>`;
  } finally {
    btn.disabled = false;
    btn.querySelector(".spinner").classList.add("hidden");
  }
}

function renderKeyResult(d) {
  if (d.error) return `<div class="key-error">${esc(d.error)}</div>`;
  if (!d.found) {
    return `<div class="key-not-found">
      <div class="big">🔍</div>
      <p><b>未找到该 Key</b></p>
      <span>${esc(d.message || "该 Key 不属于任何分组")}（RATELIMIT_ON_KEY_NOT_FOUND=passthrough 时请求直接放行）</span>
    </div>`;
  }
  const st = d.status || {};
  const meta = `
    <div class="key-meta">
      <div class="kv"><small>分组</small><b>${esc(d.group)}</b></div>
      <div class="kv"><small>限速类型</small><b>${esc(d.config.type)}</b></div>
      <div class="kv"><small>限速粒度</small><b>${esc(d.config.scope)}</b></div>
    </div>`;
  const labels = { "5h": "5 小时", "7d": "7 天", "30d": "30 天" };
  const gauges = Object.entries(st).map(([name, v]) => {
    const used = v.used, limit = v.limit;
    const pct = limit > 0 ? Math.min(100, (used / limit) * 100) : 0;
    const cls = pct >= 100 ? "danger" : pct >= 80 ? "warn" : "";
    const remain = Math.max(0, limit - used);
    return `
    <div class="gauge">
      <div class="gauge-head">
        <span class="g-label">${labels[name] || name}</span>
        <span class="g-val">已用 <b>${used}</b> / ${limit} · 剩余 ${remain}</span>
      </div>
      <div class="gauge-bar"><div class="gauge-fill ${cls}" style="width:${pct}%"></div></div>
    </div>`;
  }).join("");
  const exhausted = Object.values(st).some((v) => v.used >= v.limit);
  const tip = exhausted
    ? `<div class="key-error">该 Key 已达到上限，后续请求将返回 429</div>`
    : `<p style="color:var(--text-3);font-size:12.5px;text-align:center;">配额使用率可视化 · 多 worker 部署时 used 为当前实例可见下界</p>`;
  return meta + gauges + tip;
}

/* ================= 确认对话框 ================= */
let confirmCb = null;

function confirmAction(html, cb) {
  $("confirm-body").innerHTML = html;
  confirmCb = cb;
  confirmMask.classList.remove("hidden");
}

$("confirm-ok").addEventListener("click", async () => {
  const btn = $("confirm-ok");
  btn.disabled = true;
  btn.querySelector(".spinner").classList.remove("hidden");
  const cb = confirmCb;
  confirmCb = null;
  try { if (cb) await cb(); } finally {
    btn.disabled = false;
    btn.querySelector(".spinner").classList.add("hidden");
    confirmMask.classList.add("hidden");
  }
});
$("confirm-cancel").addEventListener("click", () => {
  confirmCb = null;
  confirmMask.classList.add("hidden");
});

/* ================= 通用模态框关闭 ================= */
function closeModals(e) {
  if (e.target === modal) modal.classList.add("hidden");
  if (e.target === keyModal) keyModal.classList.add("hidden");
  if (e.target === confirmMask) { confirmCb = null; confirmMask.classList.add("hidden"); }
  if (e.target.closest("[data-close]")) {
    const m = e.target.closest(".modal-mask");
    if (m) m.classList.add("hidden");
  }
}
document.addEventListener("click", closeModals);
document.addEventListener("keydown", (e) => {
  if (e.key === "Escape") {
    modal.classList.add("hidden");
    keyModal.classList.add("hidden");
    confirmMask.classList.add("hidden");
  }
});

/* ================= 主题 ================= */
function applyTheme() {
  document.documentElement.setAttribute("data-theme", state.theme);
}
function toggleTheme() {
  state.theme = state.theme === "dark" ? "light" : "dark";
  saveState();
  applyTheme();
}

/* ================= 登出 ================= */
function logout() {
  clearState();
  $("login-token").value = "";
  showLoginView("已退出登录");
}

/* ================= 事件绑定 & 初始化 ================= */
$("login-form").addEventListener("submit", doLogin);
$("group-form").addEventListener("submit", submitGroup);
$("key-form").addEventListener("submit", queryKey);
$("btn-create").addEventListener("click", () => openGroupModal(null));
$("btn-sync").addEventListener("click", doSync);
$("btn-check-key").addEventListener("click", () => {
  keyModal.classList.remove("hidden");
  $("key-result").classList.add("hidden");
  setTimeout(() => $("f-key").focus(), 60);
});
$("btn-logout").addEventListener("click", logout);
$("btn-theme").addEventListener("click", toggleTheme);
$("eye-toggle").addEventListener("click", () => {
  const inp = $("login-token");
  inp.type = inp.type === "password" ? "text" : "password";
});

applyTheme();
if (state.base && state.token) {
  // 已有会话：先探测 /admin/groups，通过则直接进主界面，失败回登录
  fetch(`${state.base}/admin/groups`, { headers: { Authorization: `Bearer ${state.token}` } })
    .then((r) => (r.status === 200 ? showAppView() : showLoginView()))
    .catch(() => showLoginView("无法连接网关，请确认地址"));
} else {
  showLoginView();
}
