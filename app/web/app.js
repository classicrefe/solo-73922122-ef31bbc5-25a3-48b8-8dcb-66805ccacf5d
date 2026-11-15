/* 模型试跑平台前端：原生 JS，无构建依赖 */
const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => [...root.querySelectorAll(sel)];

const esc = (s) => String(s ?? "")
  .replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;")
  .replaceAll('"', "&quot;");

let modelsCache = [];
let pollTimer = null;

function toast(msg, isError = false) {
  let el = $(".toast");
  if (!el) {
    el = document.createElement("div");
    el.className = "toast";
    document.body.appendChild(el);
  }
  el.textContent = msg;
  el.classList.toggle("error", isError);
  el.classList.add("show");
  clearTimeout(el._t);
  el._t = setTimeout(() => el.classList.remove("show"), 4000);
}

async function api(path, opts = {}) {
  const res = await fetch(path, {
    headers: opts.body instanceof FormData ? {} : { "Content-Type": "application/json" },
    ...opts,
  });
  if (res.status === 204) return null;
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(data.detail || `请求失败 (${res.status})`);
  }
  return data;
}

/* ------------------------------- Tab 切换 ------------------------------- */
$$(".tab").forEach((btn) => btn.addEventListener("click", () => {
  $$(".tab").forEach((b) => b.classList.remove("active"));
  $$(".tab-panel").forEach((p) => p.classList.remove("active"));
  btn.classList.add("active");
  $(`#tab-${btn.dataset.tab}`).classList.add("active");
  if (btn.dataset.tab === "sources") loadSources();
  if (btn.dataset.tab === "models") loadModels();
  if (btn.dataset.tab === "runs") loadRuns();
}));

/* ------------------------------- 源数据 --------------------------------- */
async function loadSources() {
  const wrap = $("#source-panels");
  wrap.innerHTML = "加载中...";
  try {
    const sources = await api("/api/sources");
    wrap.innerHTML = sources.map((s) => `
      <div class="card">
        <h3>${esc(s.table)} <span class="muted">（共 ${s.total} 行）</span></h3>
        <div class="upload-row">
          <input type="file" accept=".csv" id="csv-${esc(s.table)}" />
          <button onclick="uploadCsv('${esc(s.table)}')">上传 CSV 覆盖</button>
          <span class="muted">表头需为: ${s.columns.join(", ")}</span>
        </div>
        <div class="scroll">
          <table><thead><tr>${s.columns.map((c) => `<th>${esc(c)}</th>`).join("")}</tr></thead>
          <tbody>${s.rows.map((r) => `<tr>${s.columns.map((c) => `<td>${esc(r[c])}</td>`).join("")}</tr>`).join("")}</tbody></table>
        </div>
      </div>`).join("");
  } catch (e) {
    wrap.innerHTML = `<p class="error-msg">${esc(e.message)}</p>`;
  }
}

window.uploadCsv = async function (table) {
  const input = $(`#csv-${table}`);
  if (!input.files.length) return toast("请先选择 CSV 文件", true);
  const fd = new FormData();
  fd.append("file", input.files[0]);
  try {
    const r = await api(`/api/sources/${table}/upload`, { method: "POST", body: fd });
    toast(`${r.table} 已覆盖 ${r.loaded_rows} 行（数据版本 ${r.source_version}）`);
    loadSources();
  } catch (e) {
    toast(e.message, true);
  }
};

/* ------------------------------- 模型 ----------------------------------- */
async function loadModels() {
  const wrap = $("#model-list");
  wrap.innerHTML = "加载中...";
  try {
    modelsCache = await api("/api/models");
    const nameOf = (id) => modelsCache.find((m) => m.id === id)?.name || `#${id}`;
    if (!modelsCache.length) {
      wrap.innerHTML = `<p class="hint">暂无模型，点击「新建模型」开始。</p>`;
      return;
    }
    wrap.innerHTML = modelsCache.map((m) => `
      <div class="card">
        <h3>${esc(m.name)}
          <span class="muted">#${m.id}</span>
          ${m.upstream_ids.length ? ` · 上游: ${m.upstream_ids.map((u) => esc(nameOf(u))).join(", ")}` : ""}
        </h3>
        <pre class="sql">${esc(m.sql_text)}</pre>
        <div class="row-actions">
          <button onclick='openModelDialog(${m.id})'>编辑结构/SQL</button>
          <button onclick='openSqlQuickFix(${m.id})'>仅修正 SQL（试跑中可用）</button>
          <button class="danger" onclick='deleteModel(${m.id})'>删除</button>
        </div>
      </div>`).join("");
  } catch (e) {
    wrap.innerHTML = `<p class="error-msg">${esc(e.message)}</p>`;
  }
}

function fillUpstreamSelect(selected = []) {
  const sel = $("#m-upstreams");
  sel.innerHTML = modelsCache
    .map((m) => `<option value="${m.id}" ${selected.includes(m.id) ? "selected" : ""}>${esc(m.name)}</option>`)
    .join("");
}

window.openModelDialog = function (id) {
  $("#m-error").textContent = "";
  fillUpstreamSelect();
  if (id) {
    const m = modelsCache.find((x) => x.id === id);
    $("#dlg-title").textContent = `编辑模型 #${id}`;
    $("#m-id").value = id;
    $("#m-name").value = m.name;
    $("#m-sql").value = m.sql_text;
    fillUpstreamSelect(m.upstream_ids);
  } else {
    $("#dlg-title").textContent = "新建模型";
    $("#m-id").value = "";
    $("#m-name").value = "";
    $("#m-sql").value = "";
    fillUpstreamSelect([]);
  }
  $("#model-dialog").showModal();
};

window.openSqlQuickFix = async function (id) {
  const m = modelsCache.find((x) => x.id === id);
  $("#m-error").textContent = "";
  $("#dlg-title").textContent = `仅修正 SQL：${m.name}（名称与依赖保持不变）`;
  $("#m-id").value = id;
  $("#m-name").value = m.name;
  $("#m-sql").value = m.sql_text;
  fillUpstreamSelect(m.upstream_ids);
  $("#m-save").dataset.quick = "1";
  $("#model-dialog").showModal();
};

window.deleteModel = async function (id) {
  if (!confirm("确认删除该模型？")) return;
  try {
    await api(`/api/models/${id}`, { method: "DELETE" });
    toast("已删除");
    loadModels();
  } catch (e) {
    toast(e.message, true);
  }
};

$("#btn-new-model").addEventListener("click", () => openModelDialog(null));

$("#model-form").addEventListener("submit", async (ev) => {
  // method=dialog 默认会自动关闭；统一拦截走异步保存，取消则仅关闭
  ev.preventDefault();
  if (ev.submitter && ev.submitter.value === "cancel") {
    $("#m-save").removeAttribute("data-quick");
    $("#model-dialog").close();
    return;
  }
  const id = $("#m-id").value;
  const body = {
    name: $("#m-name").value.trim(),
    sql_text: $("#m-sql").value,
    upstream_ids: [...$("#m-upstreams").selectedOptions].map((o) => Number(o.value)),
  };
  const quick = $("#m-save").dataset.quick;
  try {
    if (quick) {
      await api(`/api/models/${id}/sql`, {
        method: "PATCH",
        body: JSON.stringify({ sql_text: body.sql_text }),
      });
    } else if (id) {
      await api(`/api/models/${id}`, { method: "PUT", body: JSON.stringify(body) });
    } else {
      await api("/api/models", { method: "POST", body: JSON.stringify(body) });
    }
    $("#m-save").removeAttribute("data-quick");
    $("#model-dialog").close();
    toast("保存成功");
    loadModels();
  } catch (e) {
    $("#m-error").textContent = e.message;
  }
});

/* ------------------------------- 试跑 ----------------------------------- */
const STATUS_BADGE = (s) => `<span class="badge ${esc(s)}">${esc(s)}</span>`;

async function loadRuns() {
  const wrap = $("#run-list");
  try {
    const runs = await api("/api/runs");
    if (!runs.length) {
      wrap.innerHTML = `<p class="hint">还没有试跑批次。</p>`;
      return;
    }
    wrap.innerHTML = runs.map((r) => `
      <div class="card">
        <h3>试跑 #${r.id} ${STATUS_BADGE(r.status)} ${r.archive_status ? ARCHIVE_BADGE(r.archive_status) : ""}</h3>
        <div class="meta">
          <span>快照: <code>${esc(r.snapshot_schema || "-")}</code></span>
          <span>源数据版本: ${r.source_version}</span>
          <span>节点: 成功 ${r.node_ok} / 失败 ${r.node_failed} / 阻塞 ${r.node_blocked} / 运行 ${r.node_running} / 待跑 ${r.node_pending}（共 ${r.node_total}）</span>
          <span>创建: ${esc(r.created_at)}</span>
          <span>结束: ${esc(r.finished_at || "-")}</span>
        </div>
        ${r.error_summary ? `<p class="error-msg">${esc(r.error_summary)}</p>` : ""}
        <div class="row-actions">
          <button onclick='openRunDetail(${r.id})'>查看详情/失败原因</button>
          ${r.status === "failed" ? `<button class="primary" onclick='retryRun(${r.id})'>重试失败节点（沿用本次快照）</button>` : ""}
          ${renderArchiveActions(r)}
        </div>
      </div>`).join("");

    const active = runs.some((r) =>
      ["initializing", "running"].includes(r.status) || r.archive_status === "sealing");
    $("#btn-start-run").disabled = active;
    schedulePoll(active);
  } catch (e) {
    wrap.innerHTML = `<p class="error-msg">${esc(e.message)}</p>`;
  }
}

/* 封存核账包：仅成功且含 daily_net_sales 净额结果的批次可封存 */
const ARCHIVE_BADGE = (s) => {
  const map = { sealed: ["success", "已封存"], sealing: ["running", "封存中…"], failed: ["failed", "封存失败"] };
  const [cls, text] = map[s] || ["pending", s];
  return `<span class="badge ${cls}">📦 ${esc(text)}</span>`;
};

function renderArchiveActions(r) {
  const sealable = r.status === "succeeded" && r.has_net_sales;
  if (r.archive_status === "sealed") {
    return `<button class="primary" onclick='downloadArchive(${r.id})'>下载核账包（${esc(r.archive_file_name || "")}）</button>`;
  }
  if (r.archive_status === "sealing") {
    return `<button disabled>封存中…</button>`;
  }
  if (r.archive_status === "failed") {
    return sealable
      ? `<button class="primary" onclick='sealArchive(${r.id})'>重新封存核账包</button>`
      : "";
  }
  return sealable
    ? `<button class="primary" onclick='sealArchive(${r.id})'>封存核账包（财务离线核账）</button>`
    : "";
}

window.sealArchive = async function (id) {
  const btn = document.activeElement;
  if (btn) { btn.disabled = true; btn.textContent = "封存中…"; }
  try {
    const a = await api(`/api/runs/${id}/archive`, { method: "POST" });
    toast(`试跑 #${id} 核账包已封存：${a.file_name}（${a.file_size} 字节）`);
    loadRuns();
    openRunDetail(id);
  } catch (e) {
    toast(e.message, true);
    loadRuns();
  }
};

window.downloadArchive = function (id) {
  // 直接走浏览器下载；服务端会先校验 SHA-256，不完整则拒绝
  window.location.href = `/api/runs/${id}/archive/download`;
};

function schedulePoll(active) {
  clearInterval(pollTimer);
  if (active) pollTimer = setInterval(() => {
    if ($("#tab-runs").classList.contains("active")) loadRuns();
  }, 1500);
}

$("#btn-start-run").addEventListener("click", async () => {
  try {
    const r = await api("/api/runs", { method: "POST" });
    toast(`试跑 #${r.run_id} 已启动，快照 ${r.snapshot_schema}`);
    loadRuns();
  } catch (e) {
    toast(e.message, true);
  }
});

window.retryRun = async function (id) {
  try {
    const r = await api(`/api/runs/${id}/retry`, { method: "POST" });
    toast(`试跑 #${id} 已重试，重置 ${r.reset_nodes} 个节点`);
    loadRuns();
    openRunDetail(id);
  } catch (e) {
    toast(e.message, true);
  }
};

window.openRunDetail = async function (id) {
  try {
    const d = await api(`/api/runs/${id}`);
    const r = d.run;
    $("#run-detail").innerHTML = `
      <h3>试跑 #${r.id} ${STATUS_BADGE(r.status)}</h3>
      <div class="meta">
        <span>快照 schema: <code>${esc(r.snapshot_schema)}</code></span>
        <span>源数据版本: ${r.source_version}</span>
        ${r.error_summary ? `<span class="error-msg">${esc(r.error_summary)}</span>` : ""}
      </div>
      <h4>节点（按依赖拓扑序）</h4>
      <div class="scroll">
      <table><thead><tr>
        <th>#</th><th>模型</th><th>状态</th><th>上游</th><th>失败原因 / 结果</th>
      </tr></thead><tbody>
      ${d.nodes.map((n, i) => `
        <tr>
          <td>${i + 1}</td>
          <td>${esc(n.model_name)}</td>
          <td>${STATUS_BADGE(n.status)}</td>
          <td>${n.upstream_ids.length ? n.upstream_ids.map((u) => {
            const up = d.nodes.find((x) => x.model_id === u);
            return esc(up ? up.model_name : `#${u}`);
          }).join(", ") : "—"}</td>
          <td>
            ${n.error ? `<pre class="sql" style="white-space:pre-wrap">${esc(n.error)}</pre>` : ""}
            ${n.status === "succeeded" ? `<button onclick='openResult(${r.id}, ${n.id})'>查看结果（${esc(n.result_table)}）</button>` : ""}
          </td>
        </tr>`).join("")}
      </tbody></table></div>
      <h4>事件流</h4>
      ${d.events.map((e) => `
        <div class="event ${esc(e.event_type)}"><strong>${esc(e.event_type)}</strong>
          ${esc(e.created_at)} — ${esc(e.message || "")}</div>`).join("")}
      ${renderArchivePanel(d)}
    `;
    $("#run-dialog").showModal();
  } catch (e) {
    toast(e.message, true);
  }
};

function renderArchivePanel(d) {
  const a = d.archive;
  if (!a) {
    return `<h4>核账包</h4><p class="hint">尚未封存。</p>`;
  }
  const head = `<h4>核账包 ${ARCHIVE_BADGE(a.status)}</h4>`;
  if (a.status === "sealed") {
    const files = (a.manifest && a.manifest.files) || [];
    return `
      ${head}
      <div class="meta">
        <span>文件: <code>${esc(a.file_name)}</code></span>
        <span>大小: ${a.file_size} 字节</span>
        <span>SHA-256: <code class="sha">${esc(a.file_sha256)}</code></span>
      </div>
      <div class="scroll"><table><thead><tr><th>包内文件</th><th>行数</th><th>大小(字节)</th><th>SHA-256</th></tr></thead>
      <tbody>${files.map((f) => `
        <tr><td><code>${esc(f.path)}</code></td>
            <td>${f.rows ?? "—"}</td>
            <td>${f.size_bytes}</td>
            <td><code class="sha">${esc(f.sha256)}</code></td></tr>`).join("")}
      </tbody></table></div>
      <div class="row-actions" style="margin-top:10px">
        <button class="primary" onclick='downloadArchive(${d.run.id})'>下载核账包 ZIP</button>
      </div>`;
  }
  if (a.status === "sealing") {
    return `${head}<p class="hint">封存进行中（若服务中断，重启后可重新封存）。</p>`;
  }
  return `${head}<p class="error-msg">${esc(a.error || "封存失败，可重新封存")}</p>
    <div class="row-actions"><button class="primary" onclick='sealArchive(${d.run.id})'>重新封存</button></div>`;
}

window.openResult = async function (runId, nodeId) {
  try {
    const d = await api(`/api/runs/${runId}/nodes/${nodeId}/result`);
    $("#result-detail").innerHTML = `
      <h3>${esc(d.model_name)} 的结果
        <span class="muted">（表 ${esc(d.result_table)}，共 ${d.total} 行，预览前 ${d.rows.length} 行）</span></h3>
      <div class="scroll">
      <table><thead><tr>${d.columns.map((c) => `<th>${esc(c)}</th>`).join("")}</tr></thead>
      <tbody>${d.rows.map((row) => `<tr>${d.columns.map((c) => `<td>${esc(row[c])}</td>`).join("")}</tr>`).join("")}</tbody></table>
      </div>`;
    $("#result-dialog").showModal();
  } catch (e) {
    toast(e.message, true);
  }
};

/* 初始加载 */
loadSources();
