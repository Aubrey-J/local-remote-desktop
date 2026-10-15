import { useEffect, useMemo, useRef, useState } from "react";
import { invoke, isTauri } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import "./App.css";

type Device = {
  id: string;
  name: string;
  address: string;
};

type ConnectionRequest = {
  requestId: string;
  peerAddress: string;
  controllerName: string;
};

type RemoteStatus = "idle" | "connecting" | "waitingApproval" | "connected" | "error";

const STORAGE_KEY = "lantern.devices";

function readDevices(): { devices: Device[]; error: string | null } {
  try {
    const saved: unknown = JSON.parse(localStorage.getItem(STORAGE_KEY) ?? "[]");
    if (!Array.isArray(saved)) {
      return { devices: [], error: "本机设备列表格式无效，已载入空列表。" };
    }

    return {
      devices: saved.filter(
        (item): item is Device =>
          typeof item === "object" &&
          item !== null &&
          typeof item.id === "string" &&
          typeof item.name === "string" &&
          typeof item.address === "string",
      ),
      error: null,
    };
  } catch (cause) {
    const detail = cause instanceof Error ? cause.message : String(cause);
    return { devices: [], error: `无法读取本机设备列表：${detail}` };
  }
}

function isValidAddress(value: string) {
  if (!value || /\s/.test(value) || value.includes("://")) return false;

  try {
    const parsed = new URL(`http://${value}`);
    const host = parsed.hostname.replace(/^\[|\]$/g, "");
    return (
      Boolean(parsed.hostname) &&
      isLanHost(host) &&
      !parsed.username &&
      !parsed.password &&
      parsed.pathname === "/" &&
      !parsed.search &&
      !parsed.hash
    );
  } catch {
    return false;
  }
}

function isLanHost(host: string) {
  if (host === "localhost") return true;
  const ipv4 = host.split(".").map(Number);
  if (ipv4.length === 4 && ipv4.every((part) => Number.isInteger(part) && part >= 0 && part <= 255)) {
    return (
      ipv4[0] === 10 ||
      ipv4[0] === 127 ||
      (ipv4[0] === 172 && ipv4[1] >= 16 && ipv4[1] <= 31) ||
      (ipv4[0] === 192 && ipv4[1] === 168) ||
      (ipv4[0] === 169 && ipv4[1] === 254)
    );
  }
  return false;
}

function App() {
  const [savedState] = useState(readDevices);
  const [devices, setDevices] = useState<Device[]>(savedState.devices);
  const [storageError, setStorageError] = useState(savedState.error);
  const [activeId, setActiveId] = useState<string | null>(null);
  const [isAdding, setIsAdding] = useState(false);
  const [name, setName] = useState("");
  const [address, setAddress] = useState("");
  const [error, setError] = useState("");
  const hasMounted = useRef(false);
  const connectionRef = useRef<WebSocket | null>(null);
  const frameUrlRef = useRef<string | null>(null);
  const lastPointerMove = useRef(0);
  const viewerRef = useRef<HTMLDivElement>(null);
  const [agentRunning, setAgentRunning] = useState(false);
  const [agentAddress, setAgentAddress] = useState("");
  const [agentStatus, setAgentStatus] = useState("");
  const [pendingRequest, setPendingRequest] = useState<ConnectionRequest | null>(null);
  const [connectionStatus, setConnectionStatus] = useState<RemoteStatus>("idle");
  const [connectionDeviceId, setConnectionDeviceId] = useState<string | null>(null);
  const [connectionError, setConnectionError] = useState("");
  const [frameUrl, setFrameUrl] = useState<string | null>(null);
  const [screenSize, setScreenSize] = useState({ width: 0, height: 0 });

  useEffect(() => {
    if (!hasMounted.current) {
      hasMounted.current = true;
      return;
    }

    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(devices));
      setStorageError(null);
    } catch (cause) {
      const detail = cause instanceof Error ? cause.message : String(cause);
      setStorageError(`无法保存本机设备列表：${detail}`);
    }
  }, [devices]);

  useEffect(() => {
    if (!isTauri()) return;
    let cancelled = false;
    let unlistenRequest: (() => void) | undefined;
    let unlistenRequestEnded: (() => void) | undefined;
    let unlistenStatus: (() => void) | undefined;

    void Promise.all([
      listen<ConnectionRequest>("remote-connection-request", (event) => {
        setPendingRequest(event.payload);
      }),
      listen<string>("remote-connection-request-ended", (event) => {
        setPendingRequest((current) =>
          current?.requestId === event.payload ? null : current,
        );
      }),
      listen<string>("agent-status", (event) => {
        setAgentStatus(event.payload);
        if (event.payload === "远程服务已停止") setAgentRunning(false);
      }),
    ]).then(([requestListener, requestEndedListener, statusListener]) => {
      if (cancelled) {
        requestListener();
        requestEndedListener();
        statusListener();
        return;
      }
      unlistenRequest = requestListener;
      unlistenRequestEnded = requestEndedListener;
      unlistenStatus = statusListener;
    }).catch((cause: unknown) => {
      const detail = cause instanceof Error ? cause.message : String(cause);
      setAgentStatus(`无法接收被控端状态：${detail}`);
    });

    return () => {
      cancelled = true;
      unlistenRequest?.();
      unlistenRequestEnded?.();
      unlistenStatus?.();
    };
  }, []);

  useEffect(() => () => {
    connectionRef.current?.close();
    if (frameUrlRef.current) URL.revokeObjectURL(frameUrlRef.current);
  }, []);

  const activeDevice = useMemo(
    () => devices.find((device) => device.id === activeId) ?? null,
    [activeId, devices],
  );

  function addDevice(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const trimmedName = name.trim();
    const trimmedAddress = address.trim();

    if (!trimmedName) {
      setError("请输入设备名称。");
      return;
    }
    if (!isValidAddress(trimmedAddress)) {
      setError("请输入有效的局域网地址，例如 192.168.1.24:4200。");
      return;
    }
    if (devices.some((device) => device.address === trimmedAddress)) {
      setError("这个地址已在设备列表中。");
      return;
    }

    const device = {
      id: crypto.randomUUID(),
      name: trimmedName,
      address: trimmedAddress,
    };
    setDevices((current) => [...current, device]);
    setActiveId(device.id);
    setName("");
    setAddress("");
    setError("");
    setIsAdding(false);
  }

  function removeDevice(id: string) {
    setDevices((current) => current.filter((device) => device.id !== id));
    if (connectionDeviceId === id) {
      disconnectFromDevice();
    }
    setActiveId((current) => (current === id ? null : current));
  }

  async function startAgent() {
    if (!isTauri()) {
      setAgentStatus("请在 Lantern 桌面应用中启动被控端；浏览器预览不支持屏幕采集。");
      return;
    }
    try {
      const address = await invoke<string>("start_agent");
      setAgentAddress(address);
      setAgentRunning(true);
      setAgentStatus("正在局域网等待连接。新连接必须在被控端本机批准。");
    } catch (cause) {
      const detail = cause instanceof Error ? cause.message : String(cause);
      setAgentStatus(`启动失败：${detail}`);
    }
  }

  async function stopAgent() {
    try {
      await invoke("stop_agent");
      setAgentRunning(false);
      setPendingRequest(null);
      setAgentStatus("远程服务已停止");
    } catch (cause) {
      const detail = cause instanceof Error ? cause.message : String(cause);
      setAgentStatus(`停止失败：${detail}`);
    }
  }

  async function answerConnection(approved: boolean) {
    if (!pendingRequest) return;
    try {
      await invoke("answer_connection_request", {
        requestId: pendingRequest.requestId,
        approved,
      });
      setPendingRequest(null);
      setAgentStatus(approved ? "已批准控制端连接。" : "已拒绝连接请求。");
    } catch (cause) {
      const detail = cause instanceof Error ? cause.message : String(cause);
      setAgentStatus(`无法处理连接请求：${detail}`);
      setPendingRequest(null);
    }
  }

  function connectToDevice(device: Device) {
    if (!isTauri()) {
      setConnectionStatus("error");
      setConnectionError("请在 Lantern 桌面应用中使用远程控制；浏览器预览不支持桌面端输入转发。");
      return;
    }
    disconnectFromDevice();
    setActiveId(device.id);
    setConnectionDeviceId(device.id);
    setConnectionStatus("connecting");
    setConnectionError("");

    try {
      const endpoint = new URL(`http://${device.address}`);
      if (!endpoint.port) endpoint.port = "4200";
      endpoint.protocol = "ws:";
      endpoint.pathname = "/ws";
      endpoint.search = "";
      endpoint.hash = "";

      const socket = new WebSocket(endpoint);
      connectionRef.current = socket;
      socket.binaryType = "arraybuffer";
      socket.onopen = () => {
        setConnectionStatus("waitingApproval");
        socket.send(JSON.stringify({ type: "hello", device_name: "Lantern 控制端" }));
      };
      socket.onmessage = (event: MessageEvent<string | ArrayBuffer>) => {
        if (typeof event.data === "string") {
          try {
            const message = JSON.parse(event.data) as {
              type?: string;
              message?: string;
              screenWidth?: number;
              screenHeight?: number;
            };
            if (message.type === "connected") {
              setConnectionStatus("connected");
              setScreenSize({
                width: message.screenWidth ?? 0,
                height: message.screenHeight ?? 0,
              });
              window.setTimeout(() => viewerRef.current?.focus(), 0);
            } else if (message.type === "error") {
              setConnectionStatus("error");
              setConnectionError(message.message ?? "远程端返回错误。");
              socket.close();
            }
          } catch {
            setConnectionStatus("error");
            setConnectionError("无法解析远程端的连接响应。");
          }
          return;
        }

        const nextUrl = URL.createObjectURL(new Blob([event.data], { type: "image/jpeg" }));
        if (frameUrlRef.current) URL.revokeObjectURL(frameUrlRef.current);
        frameUrlRef.current = nextUrl;
        setFrameUrl(nextUrl);
      };
      socket.onerror = () => {
        setConnectionStatus("error");
        setConnectionError("连接失败。请确认对端已启动、地址正确，且局域网未阻止此端口。");
      };
      socket.onclose = () => {
        connectionRef.current = null;
        if (frameUrlRef.current) URL.revokeObjectURL(frameUrlRef.current);
        frameUrlRef.current = null;
        setFrameUrl(null);
        setScreenSize({ width: 0, height: 0 });
        setConnectionStatus((status) => status === "error" ? status : "idle");
        setConnectionDeviceId((id) => id === device.id ? null : id);
      };
    } catch (cause) {
      const detail = cause instanceof Error ? cause.message : String(cause);
      setConnectionStatus("error");
      setConnectionError(`无法创建局域网连接：${detail}`);
    }
  }

  function disconnectFromDevice() {
    const socket = connectionRef.current;
    connectionRef.current = null;
    if (socket) {
      socket.onclose = null;
      socket.close();
    }
    if (frameUrlRef.current) URL.revokeObjectURL(frameUrlRef.current);
    frameUrlRef.current = null;
    setFrameUrl(null);
    setConnectionDeviceId(null);
    setConnectionStatus("idle");
    setScreenSize({ width: 0, height: 0 });
  }

  function sendInput(message: Record<string, string | number | boolean>) {
    const socket = connectionRef.current;
    if (socket?.readyState === WebSocket.OPEN && socket.bufferedAmount < 512_000) {
      socket.send(JSON.stringify(message));
    }
  }

  function sendMouseMove(event: React.MouseEvent<HTMLDivElement>) {
    const now = performance.now();
    if (now - lastPointerMove.current < 32) return;
    lastPointerMove.current = now;
    const rect = event.currentTarget.getBoundingClientRect();
    const scale = Math.min(rect.width / screenSize.width, rect.height / screenSize.height);
    const width = screenSize.width * scale;
    const height = screenSize.height * scale;
    const left = rect.left + (rect.width - width) / 2;
    const top = rect.top + (rect.height - height) / 2;
    sendInput({
      type: "mouse_move",
      x: Math.max(0, Math.min(1, (event.clientX - left) / width)),
      y: Math.max(0, Math.min(1, (event.clientY - top) / height)),
    });
  }

  function sendMouseButton(event: React.MouseEvent<HTMLDivElement>, down: boolean) {
    event.preventDefault();
    const button = event.button === 2 ? "right" : event.button === 1 ? "middle" : "left";
    sendInput({ type: "mouse_button", button, down });
  }

  function sendKey(event: React.KeyboardEvent<HTMLDivElement>, down: boolean) {
    event.preventDefault();
    sendInput({ type: "key", code: event.code, key: event.key, down });
  }

  return (
    <main className="app-shell">
      <aside className="sidebar">
        <div className="brand" aria-label="Lantern">
          <span className="brand-mark" aria-hidden="true">
            <Icon name="lantern" />
          </span>
          <span>lantern<span className="brand-period">.</span></span>
        </div>

        <div className="sidebar-section">
          <p className="section-label">工作区</p>
          <button className="nav-item nav-item-active" type="button">
            <Icon name="grid" />
            <span>设备桌面</span>
            <span className="nav-count">{devices.length}</span>
          </button>
        </div>

        <div className="device-section">
          <div className="section-heading">
            <p className="section-label">我的设备</p>
            <button
              className="icon-button small"
              type="button"
              aria-label="添加设备"
              onClick={() => setIsAdding(true)}
            >
              <Icon name="plus" />
            </button>
          </div>
          <div className="device-list">
            {devices.map((device) => (
              <button
                className={`device-nav${device.id === activeId ? " selected" : ""}`}
                key={device.id}
                onClick={() => setActiveId(device.id)}
                type="button"
              >
                <span className="device-dot" />
                <span className="device-nav-copy">
                  <span className="device-nav-name">{device.name}</span>
                  <span className="device-nav-address">{device.address}</span>
                </span>
                <span className="offline-dot" title="尚未连接" />
              </button>
            ))}
            {devices.length === 0 && (
              <p className="sidebar-empty">添加一台电脑，开始组建你的桌面。</p>
            )}
          </div>
        </div>

        <div className="sidebar-bottom">
          <div className="lan-status">
            <span className="lan-status-dot" />
            <span>仅限本地网络</span>
          </div>
          <div className="profile">
            <span className="avatar">L</span>
            <span className="profile-copy">
              <strong>本机工作区</strong>
              <span>个人设备</span>
            </span>
            <Icon name="dots" />
          </div>
        </div>
      </aside>

      <section className="main-area">
        <header className="topbar">
          <div className="breadcrumb">
            <span>工作区</span>
            <Icon name="chevron" />
            <strong>设备桌面</strong>
          </div>
          <div className="topbar-actions">
            <span className="connection-state">
              <span className={connectionStatus === "connected" ? "lan-status-dot" : "offline-dot"} />
              {connectionStatus === "connected" ? "已连接" : connectionStatus === "waitingApproval" ? "等待本机批准" : "未连接"}
            </span>
            <span className="topbar-divider" />
            <button
              className="icon-button help-button"
              type="button"
              aria-label="帮助"
              title="局域网桌面"
            >
              <Icon name="help" />
            </button>
            <span className="top-avatar">A</span>
          </div>
        </header>

        <div className="workspace">
          <div className="page-heading">
            <div>
              <div className="eyebrow"><span /> LOCAL WORKSPACE</div>
              <h1>设备桌面</h1>
              <p className="page-subtitle">在一个工作区里，专注操作你的每一台电脑。</p>
            </div>
            <div className="heading-actions">
              <button
                className={agentRunning ? "secondary-button" : "primary-button"}
                onClick={agentRunning ? stopAgent : startAgent}
                type="button"
              >
                <Icon name={agentRunning ? "close" : "desktop"} />
                {agentRunning ? "停止被控端" : "启动被控端"}
              </button>
              <button className="primary-button" onClick={() => setIsAdding(true)} type="button">
                <Icon name="plus" />
                添加设备
              </button>
            </div>
          </div>

          <div className="notice">
            <span className="notice-icon"><Icon name="info" /></span>
            <p>
              <strong>本地优先，设备互联。</strong>
              {agentRunning ? `被控端正在运行：${agentAddress}。连接需在此设备上批准。` : "两端运行同一 Lantern 应用；启动被控端后，由控制端连接。"}
            </p>
            <span className="notice-tag">局域网</span>
          </div>
          {storageError && (
            <div className="storage-error" role="alert">{storageError}</div>
          )}
          {agentStatus && (
            <div className={`agent-status${agentRunning ? " agent-status-on" : ""}`} role="status">
              <span className={agentRunning ? "lan-status-dot" : "offline-dot"} />
              <span>{agentStatus}</span>
              {agentRunning && <code>{agentAddress}</code>}
            </div>
          )}
          {connectionStatus === "error" && connectionError && (
            <div className="connection-error" role="alert">
              <span>{connectionError}</span>
              <button className="text-button" onClick={disconnectFromDevice} type="button">关闭</button>
            </div>
          )}

          <div className="section-heading desktop-heading">
            <div>
              <h2>你的设备 <span className="heading-count">{devices.length}</span></h2>
              <p>添加局域网电脑，集中查看和切换。</p>
            </div>
            <button className="view-toggle" type="button" aria-label="网格视图">
              <Icon name="grid" />
              <span>网格视图</span>
            </button>
          </div>

          {devices.length === 0 ? (
            <div className="empty-state">
              <div className="empty-illustration">
                <div className="empty-glow" />
                <div className="empty-monitor monitor-back"><span /></div>
                <div className="empty-monitor monitor-front">
                  <span className="monitor-camera" />
                  <span className="monitor-screen">
                    <i /><i /><i />
                  </span>
                  <span className="monitor-stand" />
                </div>
                <span className="sparkle sparkle-one">✦</span>
                <span className="sparkle sparkle-two">✧</span>
              </div>
              <h2>你的多设备工作区，<span>从这里开始</span></h2>
              <p>添加一台局域网电脑，把设备逐步加入同一个桌面。</p>
              <button className="primary-button empty-cta" onClick={() => setIsAdding(true)} type="button">
                <Icon name="plus" />
                添加第一台设备
              </button>
              <span className="empty-footnote"><Icon name="shield" /> 设备地址仅保存在这台电脑上</span>
            </div>
          ) : (
            <div className="device-grid">
              {devices.map((device, index) => (
                <article
                  className={`device-card${device.id === activeId ? " card-active" : ""}`}
                  key={device.id}
                >
                  {connectionDeviceId === device.id && connectionStatus === "connected" ? (
                    <div
                      className="preview-area remote-preview"
                      ref={viewerRef}
                      role="application"
                      aria-label={`${device.name} 远程桌面，可使用鼠标和键盘控制`}
                      tabIndex={0}
                      onContextMenu={(event) => event.preventDefault()}
                      onBlur={() => sendInput({ type: "release_all" })}
                      onKeyDown={(event) => sendKey(event, true)}
                      onKeyUp={(event) => sendKey(event, false)}
                      onMouseDown={(event) => sendMouseButton(event, true)}
                      onMouseLeave={() => {
                        sendInput({ type: "mouse_button", button: "left", down: false });
                        sendInput({ type: "mouse_button", button: "right", down: false });
                        sendInput({ type: "mouse_button", button: "middle", down: false });
                      }}
                      onMouseMove={sendMouseMove}
                      onMouseUp={(event) => sendMouseButton(event, false)}
                      onWheel={(event) => {
                        event.preventDefault();
                        const scale = event.deltaMode === 1
                          ? 16
                          : event.deltaMode === 2
                            ? screenSize.height
                            : 1;
                        sendInput({ type: "mouse_scroll", delta_y: event.deltaY * scale });
                      }}
                    >
                      {frameUrl && <img alt={`${device.name} 屏幕画面`} draggable={false} src={frameUrl} />}
                      {!frameUrl && <span className="preview-badge">正在接收屏幕画面…</span>}
                      <span className="remote-live"><span className="lan-status-dot" />实时画面 · 点击画面后键鼠可控</span>
                    </div>
                  ) : (
                    <button
                      className={`preview-area preview-tone-${index % 4}`}
                      onClick={() => setActiveId(device.id)}
                      type="button"
                      aria-label={`选择 ${device.name}`}
                    >
                      <span className="preview-badge">
                        <span className={connectionDeviceId === device.id && connectionStatus === "waitingApproval" ? "lan-status-dot" : "offline-dot"} />
                        {connectionDeviceId === device.id && connectionStatus === "connecting"
                          ? "连接中"
                          : connectionDeviceId === device.id && connectionStatus === "waitingApproval"
                            ? "等待被控端批准"
                            : "未连接"}
                      </span>
                      <span className="preview-content">
                        <span className="preview-display-icon"><Icon name="monitor" /></span>
                        <strong>{device.name}</strong>
                        <span>{connectionDeviceId === device.id && connectionStatus === "waitingApproval" ? "请在被控端确认连接" : "启动连接以查看屏幕并控制"}</span>
                      </span>
                      <span className="preview-resolution">— × —</span>
                    </button>
                  )}
                  <div className="device-card-footer">
                    <span className="device-card-icon"><Icon name="desktop" /></span>
                    <span className="device-card-copy">
                      <strong>{device.name}</strong>
                      <span>{device.address}</span>
                    </span>
                    {connectionDeviceId === device.id && connectionStatus !== "idle" ? (
                      <button
                        className="connect-button connect-button-muted"
                        type="button"
                        onClick={disconnectFromDevice}
                      >
                        断开
                      </button>
                    ) : (
                      <button
                        className="connect-button"
                        type="button"
                        onClick={() => connectToDevice(device)}
                      >
                        连接控制
                      </button>
                    )}
                    <button
                      className="icon-button card-menu"
                      type="button"
                      aria-label={`移除 ${device.name}`}
                      title="从列表移除"
                      onClick={() => removeDevice(device.id)}
                    >
                      <Icon name="dots" />
                    </button>
                  </div>
                </article>
              ))}
            </div>
          )}

          {activeDevice && (
            <div className="selection-bar">
              <span className="selection-check"><Icon name="check" /></span>
              <span><strong>{activeDevice.name}</strong> 已选中</span>
              <span className="selection-address">{activeDevice.address}</span>
              <span className="selection-hint">
                {connectionDeviceId === activeDevice.id && connectionStatus === "connected"
                  ? `${screenSize.width} × ${screenSize.height} · 画面已连接`
                  : "连接后查看画面并转发键鼠操作"}
              </span>
            </div>
          )}

          <footer className="workspace-footer">
            <span>LAN-only <i /> 无云端中转</span>
            <span>Lantern Desktop <i /> 早期预览</span>
          </footer>
        </div>
      </section>

      {isAdding && (
        <div
          className="modal-backdrop"
          onMouseDown={(event) => {
            if (event.target === event.currentTarget) setIsAdding(false);
          }}
        >
          <form className="device-dialog" onSubmit={addDevice}>
            <button
              className="dialog-close icon-button"
              type="button"
              aria-label="关闭"
              onClick={() => {
                setIsAdding(false);
                setError("");
              }}
            >
              <Icon name="close" />
            </button>
            <span className="dialog-icon"><Icon name="desktop" /></span>
            <h2>添加局域网设备</h2>
            <p>输入远程电脑的名称和局域网地址。两台设备需处于同一网络。</p>
            <label htmlFor="device-name">设备名称</label>
            <input
              autoFocus
              id="device-name"
              maxLength={48}
              onChange={(event) => setName(event.currentTarget.value)}
              placeholder="例如：开发机"
              value={name}
            />
            <label htmlFor="device-address">局域网地址</label>
            <input
              id="device-address"
              onChange={(event) => setAddress(event.currentTarget.value)}
              placeholder="192.168.1.24:4200"
              value={address}
            />
            <span className="field-hint">只接受本地手动添加；自动发现功能尚未启用。</span>
            {error && <p className="form-error" role="alert">{error}</p>}
            <div className="dialog-actions">
              <button
                className="secondary-button"
                onClick={() => {
                  setIsAdding(false);
                  setError("");
                }}
                type="button"
              >
                取消
              </button>
              <button className="primary-button" type="submit">保存设备</button>
            </div>
          </form>
        </div>
      )}
      {pendingRequest && (
        <div className="modal-backdrop">
          <section className="device-dialog approval-dialog" aria-labelledby="approval-title">
            <span className="dialog-icon"><Icon name="shield" /></span>
            <h2 id="approval-title">允许远程控制？</h2>
            <p>
              {pendingRequest.controllerName}（{pendingRequest.peerAddress}）请求查看屏幕并控制键盘和鼠标。
              仅在你批准后才会开始屏幕传输和输入控制。
            </p>
            <div className="dialog-actions">
              <button className="secondary-button" onClick={() => void answerConnection(false)} type="button">
                拒绝
              </button>
              <button className="primary-button" onClick={() => void answerConnection(true)} type="button">
                允许本次连接
              </button>
            </div>
          </section>
        </div>
      )}
    </main>
  );
}

function Icon({ name }: { name: string }) {
  const common = {
    fill: "none",
    stroke: "currentColor",
    strokeLinecap: "round" as const,
    strokeLinejoin: "round" as const,
    strokeWidth: 1.8,
    viewBox: "0 0 24 24",
  };

  const paths: Record<string, React.ReactNode> = {
    lantern: <><path d="M8 8h8l1.2 10H6.8L8 8Z" /><path d="M9 8c0-2 1.2-3.5 3-3.5S15 6 15 8M10 12v3m4-3v3M5 20h14" /></>,
    grid: <><rect x="4" y="4" width="6" height="6" rx="1.2" /><rect x="14" y="4" width="6" height="6" rx="1.2" /><rect x="4" y="14" width="6" height="6" rx="1.2" /><rect x="14" y="14" width="6" height="6" rx="1.2" /></>,
    plus: <><path d="M12 5v14M5 12h14" /></>,
    dots: <><circle cx="5" cy="12" r="1" /><circle cx="12" cy="12" r="1" /><circle cx="19" cy="12" r="1" /></>,
    chevron: <path d="m9 18 6-6-6-6" />,
    help: <><circle cx="12" cy="12" r="9" /><path d="M9.8 9a2.3 2.3 0 1 1 3.8 1.7c-1 .9-1.6 1.1-1.6 2.6m0 3h.01" /></>,
    info: <><circle cx="12" cy="12" r="9" /><path d="M12 11v5m0-8h.01" /></>,
    monitor: <><rect x="3" y="4" width="18" height="13" rx="2" /><path d="M8 21h8m-4-4v4" /></>,
    desktop: <><rect x="3" y="4" width="18" height="13" rx="2" /><path d="M8 21h8m-4-4v4" /><path d="M7 8h10" /></>,
    shield: <><path d="M12 3 19 6v5c0 4.5-3 7.5-7 10-4-2.5-7-5.5-7-10V6l7-3Z" /><path d="m9 12 2 2 4-4" /></>,
    check: <path d="m5 12 4 4L19 6" />,
    close: <><path d="m18 6-12 12M6 6l12 12" /></>,
  };

  return (
    <svg aria-hidden="true" {...common}>
      {paths[name] ?? null}
    </svg>
  );
}

export default App;
