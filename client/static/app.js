// Giao diện web – JavaScript thuần, chạy trong trình duyệt (bước 9).
// Trình duyệt KHÔNG mở được socket TCP/UDP, nên mọi thứ đi qua HTTP tới tiến trình client Python.
// Không dùng WebSocket (đề cấm) -> lấy tin mới bằng polling GET /api/events?since=<id>.
// Trạng thái: chạy được ngay khi các API phía Python đã xong. TODO[B-D3]: hoàn thiện, làm đẹp, kiểm tra lỗi.

const POLL_MS = 500;
const $ = (sel) => document.querySelector(sel);

const ui = {
  lastEventId: 0,
  me: null,
  state: "DISCONNECTED",
  users: [],
  active: "*",                 // "*" = phòng chung, hoặc username = chat riêng
  messages: { "*": [] },       // khóa: "*" hoặc username người kia
  unread: {},
  transfers: new Map(),        // khóa: file_id hoặc client_ref
  stats: null,
};

// ------------------------------------------------------------------ gọi API
async function api(method, path, body, rawBody) {
  const opts = { method, headers: {} };
  if (rawBody !== undefined) {
    opts.body = rawBody;
    opts.headers["Content-Type"] = "application/octet-stream";
  } else if (body !== undefined) {
    opts.body = JSON.stringify(body);
    opts.headers["Content-Type"] = "application/json";
  }
  const res = await fetch(path, opts);
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || data.reason || `HTTP ${res.status}`);
  return data;
}

// ------------------------------------------------------------------ đăng nhập
async function connectAndAuth(register) {
  const server = $("#f-server").value.trim();
  const username = $("#f-user").value.trim();
  const password = $("#f-pass").value;
  $("#login-error").textContent = "";
  try {
    if (ui.state === "DISCONNECTED") await api("POST", "/api/connect", { server });
    if (register) await api("POST", "/api/register", { username, password });
    await api("POST", "/api/login", { username, password });
  } catch (err) {
    $("#login-error").textContent = err.message;
  }
}

$("#login-form").addEventListener("submit", (e) => { e.preventDefault(); connectAndAuth(false); });
$("#btn-register").addEventListener("click", () => {
  if ($("#login-form").reportValidity()) connectAndAuth(true);
});
$("#btn-logout").addEventListener("click", () => api("POST", "/api/logout").catch(() => {}));

// ------------------------------------------------------------------ chat
$("#send-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const input = $("#msg-input");
  const text = input.value.trim();
  if (!text) return;
  input.value = "";
  try {
    await api("POST", "/api/send", { to: ui.active, text });
  } catch (err) {
    addSystem(ui.active, `Không gửi được: ${err.message}`);
  }
});

function chatKeyOf(msg) {
  if (msg.to === "*") return "*";
  return msg.from === ui.me ? msg.to : msg.from;
}

function addMessage(msg) {
  const key = chatKeyOf(msg);
  (ui.messages[key] ||= []).push(msg);
  if (key !== ui.active) ui.unread[key] = (ui.unread[key] || 0) + 1;
}

function addSystem(key, text) {
  (ui.messages[key] ||= []).push({ system: true, text, ts: Date.now() / 1000 });
}

function selectChat(key) {
  ui.active = key;
  ui.unread[key] = 0;
  $("#chat-title").textContent = key === "*" ? "# Phòng chung" : `Nhắn riêng với ${key}`;
  $("#file-input").disabled = key === "*";
  renderAll();
  $("#msg-input").focus();
}

document.querySelector('[data-chat="*"]').addEventListener("click", () => selectChat("*"));

// ------------------------------------------------------------------ file
$("#file-input").addEventListener("change", async (e) => {
  const file = e.target.files[0];
  e.target.value = "";
  if (!file) return;
  if (ui.active === "*") { addSystem("*", "Chọn một người trong danh sách để gửi file."); renderAll(); return; }
  const q = new URLSearchParams({ to: ui.active, name: file.name });
  try {
    await api("POST", `/api/upload?${q}`, undefined, file);   // gửi byte thô của file trong body
    addSystem(ui.active, `Đang chờ ${ui.active} nhận file ${file.name}`);
  } catch (err) {
    addSystem(ui.active, `Không gửi được file: ${err.message}`);
  }
  renderAll();
});

function upsertTransfer(t) {
  ui.transfers.set(t.file_id ?? t.client_ref, t);
  if (t.file_id != null && t.client_ref) ui.transfers.delete(t.client_ref);
}

// ------------------------------------------------------------------ polling sự kiện
function handleEvent(ev) {
  switch (ev.kind) {
    case "message": addMessage(ev.message); break;
    case "user_join": addSystem("*", `${ev.user.username} vừa vào`); break;
    case "user_leave": addSystem("*", `${ev.username} đã rời đi`); break;
    case "error": addSystem(ui.active, ev.reason); break;
    case "file_incoming":
      upsertTransfer(ev.transfer);
      addSystem(ev.transfer.peer, `${ev.transfer.peer} muốn gửi file ${ev.transfer.name}`);
      break;
    case "file_progress":
    case "file_done": upsertTransfer(ev.transfer); break;
  }
}

async function poll() {
  try {
    const data = await api("GET", `/api/events?since=${ui.lastEventId}`);
    for (const ev of data.events) {
      ui.lastEventId = ev.id;
      handleEvent(ev);
    }
    applyState(data.state);
  } catch {
    applyState({ state: "DISCONNECTED", users: [], stats: null, files: [] });
    $("#conn-state").textContent = "Mất kết nối tới tiến trình client";
  }
  setTimeout(poll, POLL_MS);
}

function applyState(s) {
  ui.state = s.state;
  ui.me = s.me;
  ui.users = (s.users || []).filter((u) => u.username !== s.me);
  ui.stats = s.stats;
  for (const t of s.files || []) upsertTransfer(t);
  renderAll();
}

// ------------------------------------------------------------------ hiển thị
function el(tag, props = {}, ...children) {
  const node = Object.assign(document.createElement(tag), props);
  for (const c of children) node.append(c);
  return node;
}

const fmtTime = (ts) => new Date(ts * 1000).toLocaleTimeString("vi-VN", { hour: "2-digit", minute: "2-digit" });
const fmtSize = (n) => (n < 1024 ? `${n} B` : n < 1 << 20 ? `${(n / 1024).toFixed(1)} KB` : `${(n / (1 << 20)).toFixed(1)} MB`);

function renderAll() {
  const loggedIn = ui.state === "AUTHENTICATED";
  $("#login-view").hidden = loggedIn;
  $("#app-view").hidden = !loggedIn;
  if (!loggedIn) return;
  renderTopbar();
  renderUsers();
  renderMessages();
  renderStats();
  renderTransfers();
}

function renderTopbar() {
  const conn = $("#conn-state");
  const rtt = ui.stats?.rtt_ms ?? 0;
  conn.className = "conn " + (rtt > 200 || (ui.stats?.loss ?? 0) > 0.05 ? "slow" : "up");
  conn.textContent = ui.stats?.encrypted ? "Đã kết nối, mã hóa AES-GCM" : "Đã kết nối";
  $("#me").textContent = ui.me || "";
}

function renderUsers() {
  $("#online-count").textContent = `(${ui.users.length})`;
  document.querySelector('[data-chat="*"]').classList.toggle("active", ui.active === "*");
  const list = $("#user-list");
  list.replaceChildren(...ui.users.map((u) => {
    const btn = el("button", { className: ui.active === u.username ? "active" : "" }, u.username);
    if (ui.unread[u.username]) btn.prepend(el("span", { className: "badge" }, String(ui.unread[u.username])));
    btn.append(el("span", { className: "addr" }, `${u.ip}:${u.port}`));
    btn.addEventListener("click", () => selectChat(u.username));
    return el("li", {}, btn);
  }));
}

function renderMessages() {
  const box = $("#messages");
  const atBottom = box.scrollHeight - box.scrollTop - box.clientHeight < 40;
  const msgs = ui.messages[ui.active] || [];
  if (!msgs.length) {
    box.replaceChildren(el("li", { className: "empty" }, "Chưa có tin nhắn. Hãy gửi lời chào."));
    return;
  }
  box.replaceChildren(...msgs.map((m) => {
    if (m.system) return el("li", { className: "system" }, m.text);
    return el("li", { className: m.from === ui.me ? "mine" : "" },
      el("span", { className: "who" }, m.from), el("time", {}, fmtTime(m.ts)),
      el("p", { className: "text" }, m.text));   // textContent -> an toàn trước XSS
  }));
  if (atBottom) box.scrollTop = box.scrollHeight;
}

function renderStats() {
  const s = ui.stats;
  const rows = s ? [
    ["RTT", `${s.rtt_ms} ms`], ["RTO", `${s.rto_ms} ms`], ["cwnd", `${s.cwnd} gói (${s.cc})`],
    ["Đang bay", `${s.in_flight} gói`], ["Hàng đợi", `${s.queued} đoạn`],
    ["Mất gói", `${(s.loss * 100).toFixed(1)}%`], ["Đã gửi", `${s.sent} gói`], ["Gói lỗi", s.bad_packets],
  ] : [["Trạng thái", ui.state]];
  $("#stats").replaceChildren(...rows.flatMap(([k, v]) => [el("dt", {}, k), el("dd", {}, String(v))]));
}

const STATUS_TEXT = {
  offered: "Chờ đồng ý", accepted: "Đã đồng ý", sending: "Đang gửi", receiving: "Đang nhận",
  done: "Hoàn tất", rejected: "Bị từ chối", failed: "Lỗi",
};

function renderTransfers() {
  const items = [...ui.transfers.values()].reverse();
  if (!items.length) {
    $("#transfers").replaceChildren(el("li", { className: "empty" }, "Chưa có file nào."));
    return;
  }
  $("#transfers").replaceChildren(...items.map((t) => {
    const dir = t.direction === "out" ? `Gửi tới ${t.peer}` : `Từ ${t.peer}`;
    const li = el("li", {},
      el("div", { className: "name" }, t.name),
      el("div", { className: "meta" }, `${dir}, ${fmtSize(t.size)}`),
      el("progress", { max: 1, value: t.progress }),
      el("div", { className: "meta" + (t.status === "failed" ? " failed" : "") },
        `${STATUS_TEXT[t.status] || t.status} ${Math.round(t.progress * 100)}%${t.error ? `, ${t.error}` : ""}`));
    if (t.direction === "in" && t.status === "offered") {
      const ok = el("button", { className: "primary" }, "Nhận");
      const no = el("button", {}, "Từ chối");
      ok.onclick = () => api("POST", "/api/files/accept", { file_id: t.file_id }).catch((e) => addSystem(t.peer, e.message));
      no.onclick = () => api("POST", "/api/files/reject", { file_id: t.file_id }).catch(() => {});
      li.append(el("div", { className: "actions" }, ok, no));
    }
    if (t.direction === "in" && t.status === "done") {
      li.append(el("div", { className: "actions" }, el("a", { href: `/api/files/download?id=${t.file_id}` }, "Tải về")));
    }
    return li;
  }));
}

// ------------------------------------------------------------------ khởi động
api("GET", "/api/state").then((s) => { if (s.default_server) $("#f-server").value = s.default_server; }).catch(() => {});
renderAll();
poll();
