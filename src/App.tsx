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

type VideoQuality = "quality" | "balanced" | "smooth";

type LocalDeviceInfo = {
  name: string;
  address: string;
};

type TrustedDevice = { ipAddress: string };
type DeviceLayout = "grid" | "tile" | "list";
type FileOffer = { transferId: string; fileName: string; size: number };
type ClipboardOffer = { text: string };

type RemoteStatus = "idle" | "connecting" | "waitingApproval" | "connected" | "error";

const STORAGE_KEY = "lantern.devices";
const QUALITY_STORAGE_KEY = "lantern.video-quality";
const LAYOUT_STORAGE_KEY = "lantern.device-layout";
const CLIPBOARD_STORAGE_KEY = "lantern.clipboard-enabled";
const DEFAULT_PORT = "4200";

function readDeviceLayout(): DeviceLayout {
  const value = localStorage.getItem(LAYOUT_STORAGE_KEY);
  return value === "tile" || value === "list" ? value : "grid";
}

function readClipboardEnabled() {
  return localStorage.getItem(CLIPBOARD_STORAGE_KEY) === "true";
}

function readVideoQuality(): VideoQuality {
  const value = localStorage.getItem(QUALITY_STORAGE_KEY);
  return value === "quality" || value === "smooth" ? value : "balanced";
}

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

function normalizeAddress(value: string) {
  const parsed = new URL(`http://${value}`);
  if (!parsed.port) parsed.port = DEFAULT_PORT;
  return parsed.host;
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
  const [openDeviceMenuId, setOpenDeviceMenuId] = useState<string | null>(null);
  const [pendingRemoval, setPendingRemoval] = useState<Device | null>(null);
  const [videoQuality, setVideoQuality] = useState<VideoQuality>(() => {
    try {
      return readVideoQuality();
    } catch {
      return "balanced";
    }
  });
  const [showLocalInfo, setShowLocalInfo] = useState(false);
  const [localDeviceInfo, setLocalDeviceInfo] = useState<LocalDeviceInfo | null>(null);
  const [localInfoError, setLocalInfoError] = useState("");
  const [isLoadingLocalInfo, setIsLoadingLocalInfo] = useState(false);
  const [copyAddressStatus, setCopyAddressStatus] = useState("");
  const [showSettings, setShowSettings] = useState(false);
  const [deviceLayout, setDeviceLayout] = useState<DeviceLayout>(() => {
    try {
      return readDeviceLayout();
    } catch {
      return "grid";
    }
  });
  const [clipboardEnabled, setClipboardEnabled] = useState(() => {
    try {
      return readClipboardEnabled();
    } catch {
      return false;
    }
  });
  const [trustedDevices, setTrustedDevices] = useState<TrustedDevice[]>([]);
  const [trustedDevicesError, setTrustedDevicesError] = useState("");
  const [trustCurrentDevice, setTrustCurrentDevice] = useState(false);
  const [pendingFileOffer, setPendingFileOffer] = useState<FileOffer | null>(null);
  const [pendingClipboardOffer, setPendingClipboardOffer] = useState<ClipboardOffer | null>(null);
  const [transferStatus, setTransferStatus] = useState("");
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [name, setName] = useState("");
  const [address, setAddress] = useState("");
  const [error, setError] = useState("");
  const hasMounted = useRef(false);
  const connectionRef = useRef<WebSocket | null>(null);
  const peerConnectionRef = useRef<RTCPeerConnection | null>(null);
  const controlChannelRef = useRef<RTCDataChannel | null>(null);
  const pendingFileSendsRef = useRef<Map<string, File>>(new Map());
  const clipboardSessionAllowedRef = useRef(false);
  const remoteStreamRef = useRef<MediaStream | null>(null);
  const pendingRemoteCandidatesRef = useRef<RTCIceCandidateInit[]>([]);
  const lastPointerMove = useRef(0);
  const viewerRef = useRef<HTMLDivElement>(null);
  const remoteVideoRef = useRef<HTMLVideoElement>(null);
  const [agentRunning, setAgentRunning] = useState(false);
  const [agentAddress, setAgentAddress] = useState("");
  const [agentStatus, setAgentStatus] = useState("");
  const [pendingRequest, setPendingRequest] = useState<ConnectionRequest | null>(null);
  const [connectionStatus, setConnectionStatus] = useState<RemoteStatus>("idle");
  const [connectionDeviceId, setConnectionDeviceId] = useState<string | null>(null);
  const [connectionError, setConnectionError] = useState("");
  const [encoderStatus, setEncoderStatus] = useState("");
  const [screenSize, setScreenSize] = useState({ width: 0, height: 0 });
  const [isRemoteFullscreen, setIsRemoteFullscreen] = useState(false);

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
    try {
      localStorage.setItem(QUALITY_STORAGE_KEY, videoQuality);
    } catch (cause) {
      const detail = cause instanceof Error ? cause.message : String(cause);
      setStorageError(`无法保存画质偏好：${detail}`);
    }
  }, [videoQuality]);

  useEffect(() => {
    try {
      localStorage.setItem(LAYOUT_STORAGE_KEY, deviceLayout);
      localStorage.setItem(CLIPBOARD_STORAGE_KEY, String(clipboardEnabled));
    } catch (cause) {
      const detail = cause instanceof Error ? cause.message : String(cause);
      setStorageError(`无法保存应用偏好：${detail}`);
    }
  }, [deviceLayout, clipboardEnabled]);

  useEffect(() => {
    if (!isTauri()) return;
    let cancelled = false;
    let unlistenRequest: (() => void) | undefined;
    let unlistenRequestEnded: (() => void) | undefined;
    let unlistenStatus: (() => void) | undefined;
    let unlistenClipboard: (() => void) | undefined;
    let unlistenFileOffer: (() => void) | undefined;
    let unlistenFileReceived: (() => void) | undefined;

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
      listen<ClipboardOffer>("remote-clipboard-offer", (event) => {
        if (clipboardSessionAllowedRef.current) {
          void navigator.clipboard.writeText(event.payload.text)
            .then(() => setTransferStatus("已将对端文本写入本机剪贴板（本次连接已授权）。"))
            .catch((cause: unknown) => {
              const detail = cause instanceof Error ? cause.message : String(cause);
              setTransferStatus(`无法写入剪贴板：${detail}`);
            });
        } else {
          setPendingClipboardOffer(event.payload);
        }
      }),
      listen<FileOffer>("remote-file-offer", (event) => setPendingFileOffer(event.payload)),
      listen<{ fileName: string; savedPath: string }>("remote-file-received", (event) => {
        setTransferStatus(`已接收 ${event.payload.fileName}，保存至 ${event.payload.savedPath}`);
      }),
    ]).then(([requestListener, requestEndedListener, statusListener, clipboardListener, fileListener, receivedListener]) => {
      if (cancelled) {
        requestListener();
        requestEndedListener();
        statusListener();
        clipboardListener();
        fileListener();
        receivedListener();
        return;
      }
      unlistenRequest = requestListener;
      unlistenRequestEnded = requestEndedListener;
      unlistenStatus = statusListener;
      unlistenClipboard = clipboardListener;
      unlistenFileOffer = fileListener;
      unlistenFileReceived = receivedListener;
    }).catch((cause: unknown) => {
      const detail = cause instanceof Error ? cause.message : String(cause);
      setAgentStatus(`无法接收被控端状态：${detail}`);
    });

    return () => {
      cancelled = true;
      unlistenRequest?.();
      unlistenRequestEnded?.();
      unlistenStatus?.();
      unlistenClipboard?.();
      unlistenFileOffer?.();
      unlistenFileReceived?.();
    };
  }, []);

  useEffect(() => {
    if (!isTauri() || !agentRunning) return;
    let cancelled = false;
    const pollPendingRequest = () => {
      void invoke<ConnectionRequest | null>("get_pending_connection_request")
        .then((request) => {
          if (!cancelled) setPendingRequest(request);
        })
        .catch((cause: unknown) => {
          if (cancelled) return;
          const detail = cause instanceof Error ? cause.message : String(cause);
          setAgentStatus(`无法查询待批准的连接：${detail}`);
        });
    };
    pollPendingRequest();
    const timer = window.setInterval(pollPendingRequest, 750);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [agentRunning]);

  useEffect(() => () => {
    connectionRef.current?.close();
    controlChannelRef.current?.close();
    peerConnectionRef.current?.close();
  }, []);

  useEffect(() => {
    const updateFullscreenState = () => {
      setIsRemoteFullscreen(document.fullscreenElement === viewerRef.current);
    };
    document.addEventListener("fullscreenchange", updateFullscreenState);
    return () => document.removeEventListener("fullscreenchange", updateFullscreenState);
  }, []);

  useEffect(() => {
    const video = remoteVideoRef.current;
    const stream = remoteStreamRef.current;
    if (video && stream && video.srcObject !== stream) {
      video.srcObject = stream;
      void video.play().catch(() => {
        setConnectionError("无法播放远程视频流，请检查应用的媒体权限。");
      });
    }
  }, [connectionStatus]);

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
    const normalizedAddress = normalizeAddress(trimmedAddress);
    if (devices.some(
      (device) => isValidAddress(device.address) && normalizeAddress(device.address) === normalizedAddress,
    )) {
      setError("这个地址已在设备列表中。");
      return;
    }

    const device = {
      id: crypto.randomUUID(),
      name: trimmedName,
      address: normalizedAddress,
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

  async function toggleRemoteFullscreen() {
    const viewer = viewerRef.current;
    if (!viewer) return;
    try {
      if (document.fullscreenElement === viewer) {
        await document.exitFullscreen();
      } else {
        await viewer.requestFullscreen();
      }
    } catch (cause) {
      const detail = cause instanceof Error ? cause.message : String(cause);
      setConnectionError(`无法切换全屏显示：${detail}`);
    }
  }

  async function toggleLocalInfo() {
    const shouldOpen = !showLocalInfo;
    setShowLocalInfo(shouldOpen);
    setCopyAddressStatus("");
    if (!shouldOpen || !isTauri()) return;

    setIsLoadingLocalInfo(true);
    setLocalInfoError("");
    try {
      const info = await invoke<LocalDeviceInfo>("get_local_device_info");
      setLocalDeviceInfo(info);
    } catch (cause) {
      const detail = cause instanceof Error ? cause.message : String(cause);
      setLocalInfoError(`无法读取本机信息：${detail}`);
    } finally {
      setIsLoadingLocalInfo(false);
    }
  }

  async function copyLocalAddress() {
    if (!localDeviceInfo) return;
    try {
      await navigator.clipboard.writeText(localDeviceInfo.address);
      setCopyAddressStatus("已复制地址");
    } catch (cause) {
      const detail = cause instanceof Error ? cause.message : String(cause);
      setCopyAddressStatus(`复制失败：${detail}`);
    }
  }

  async function openSettings() {
    setShowSettings(true);
    if (!isTauri()) return;
    setTrustedDevicesError("");
    try {
      setTrustedDevices(await invoke<TrustedDevice[]>("get_trusted_devices"));
    } catch (cause) {
      const detail = cause instanceof Error ? cause.message : String(cause);
      setTrustedDevicesError(`无法读取信任设备：${detail}`);
    }
  }

  async function removeTrustedDevice(ipAddress: string) {
    try {
      await invoke("remove_trusted_device", { ipAddress });
      setTrustedDevices((current) => current.filter((device) => device.ipAddress !== ipAddress));
    } catch (cause) {
      const detail = cause instanceof Error ? cause.message : String(cause);
      setTrustedDevicesError(`无法移除信任设备：${detail}`);
    }
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
        trustDevice: approved && trustCurrentDevice,
      });
      setPendingRequest(null);
      setTrustCurrentDevice(false);
      setAgentStatus(approved ? "已批准控制端连接。" : "已拒绝连接请求。");
    } catch (cause) {
      const detail = cause instanceof Error ? cause.message : String(cause);
      setAgentStatus(`无法处理连接请求：${detail}`);
      setPendingRequest(null);
      setTrustCurrentDevice(false);
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
    setEncoderStatus("");

    try {
      const endpoint = new URL(`http://${device.address}`);
      if (!endpoint.port) endpoint.port = "4200";
      endpoint.protocol = "ws:";
      endpoint.pathname = "/ws";
      endpoint.search = "";
      endpoint.hash = "";

      const socket = new WebSocket(endpoint);
      connectionRef.current = socket;
      socket.onopen = () => {
        setConnectionStatus("waitingApproval");
        socket.send(JSON.stringify({
          type: "hello",
          device_name: "Lantern 控制端",
          quality: videoQuality,
          clipboard_enabled: clipboardEnabled,
        }));
      };
      socket.onmessage = (event: MessageEvent<string>) => {
        let message: {
          type?: string;
          message?: string;
          status?: string;
          sdp?: string;
          candidate?: string;
          sdpMid?: string | null;
          sdpMLineIndex?: number | null;
          transfer_id?: string;
          accepted?: boolean;
        };
        try {
          message = JSON.parse(event.data) as typeof message;
        } catch {
          setConnectionStatus("error");
          setConnectionError("无法解析远程端的信令响应。");
          socket.close();
          return;
        }

        if (message.type === "approved") {
          setConnectionStatus("connecting");
          void startWebRtcConnection(socket);
        } else if (message.type === "answer" && message.sdp) {
          const peer = peerConnectionRef.current;
          if (!peer) return;
          void peer.setRemoteDescription({ type: "answer", sdp: message.sdp })
            .then(async () => {
              const candidates = pendingRemoteCandidatesRef.current.splice(0);
              await Promise.all(candidates.map((candidate) => peer.addIceCandidate(candidate)));
            })
            .catch((cause: unknown) => {
              if (connectionRef.current !== socket || peerConnectionRef.current !== peer) return;
              const detail = cause instanceof Error ? cause.message : String(cause);
              setConnectionStatus("error");
              setConnectionError(`无法协商 WebRTC 媒体连接：${detail}`);
            });
        } else if (message.type === "ice_candidate" && message.candidate) {
          const candidate: RTCIceCandidateInit = {
            candidate: message.candidate,
            sdpMid: message.sdpMid,
            sdpMLineIndex: message.sdpMLineIndex,
          };
          const peer = peerConnectionRef.current;
          if (!peer) return;
          if (peer.remoteDescription) {
            void peer.addIceCandidate(candidate).catch((cause: unknown) => {
              if (connectionRef.current !== socket || peerConnectionRef.current !== peer) return;
              const detail = cause instanceof Error ? cause.message : String(cause);
              setConnectionStatus("error");
              setConnectionError(`无法添加 WebRTC 网络候选：${detail}`);
            });
          } else {
            pendingRemoteCandidatesRef.current.push(candidate);
          }
        } else if (message.type === "connected") {
          setConnectionStatus("connected");
          window.setTimeout(() => viewerRef.current?.focus(), 0);
        } else if (message.type === "encoder_status" && message.status) {
          setEncoderStatus(message.status);
        } else if (message.type === "file_transfer_decision" && message.transfer_id) {
          const file = pendingFileSendsRef.current.get(message.transfer_id);
          if (!file) return;
          if (!message.accepted) {
            pendingFileSendsRef.current.delete(message.transfer_id);
            setTransferStatus(`${file.name}：对端拒绝接收。`);
          } else {
            void transmitFile(socket, message.transfer_id, file);
          }
        } else if (message.type === "file_transfer_complete") {
          setTransferStatus("文件已成功发送。");
        } else if (message.type === "clipboard_error" && message.message) {
          setTransferStatus(message.message);
        } else if (message.type === "error") {
          setConnectionStatus("error");
          setConnectionError(message.message ?? "远程端返回错误。");
          socket.close();
        } else if (message.type === "closed") {
          setConnectionStatus("idle");
          socket.close();
        }
      };
      socket.onerror = () => {
        if (connectionRef.current !== socket) return;
        setConnectionStatus("error");
        setConnectionError("连接失败。请确认对端已启动、地址正确，且局域网未阻止 TCP 4200。");
      };
      socket.onclose = () => {
        if (connectionRef.current !== socket) return;
        connectionRef.current = null;
        clipboardSessionAllowedRef.current = false;
        setPendingClipboardOffer(null);
        controlChannelRef.current?.close();
        controlChannelRef.current = null;
        const peer = peerConnectionRef.current;
        if (peer) {
          peer.onconnectionstatechange = null;
          peer.onicecandidate = null;
          peer.ontrack = null;
          peer.close();
        }
        peerConnectionRef.current = null;
        remoteStreamRef.current = null;
        pendingRemoteCandidatesRef.current = [];
        pendingFileSendsRef.current.clear();
        if (remoteVideoRef.current) remoteVideoRef.current.srcObject = null;
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

  async function startWebRtcConnection(socket: WebSocket) {
    const peer = new RTCPeerConnection({ iceServers: [] });
    peerConnectionRef.current = peer;
    peer.addTransceiver("video", { direction: "recvonly" });

    const controlChannel = peer.createDataChannel("control");
    controlChannelRef.current = controlChannel;
    controlChannel.onopen = () => {
      if (peerConnectionRef.current !== peer) return;
      if (peer.connectionState === "connected") setConnectionStatus("connected");
    };
    controlChannel.onerror = () => {
      if (peerConnectionRef.current !== peer) return;
      setConnectionError("WebRTC 键鼠控制通道发生错误。");
    };

    let offerSent = false;
    const earlyCandidates: RTCIceCandidate[] = [];
    const sendCandidate = (candidate: RTCIceCandidate) => {
      if (socket.readyState === WebSocket.OPEN) {
        socket.send(JSON.stringify({
          type: "ice_candidate",
          candidate: candidate.candidate,
          sdpMid: candidate.sdpMid,
          sdpMLineIndex: candidate.sdpMLineIndex,
        }));
      }
    };
    peer.onicecandidate = (event) => {
      if (peerConnectionRef.current !== peer) return;
      if (!event.candidate) return;
      if (offerSent) sendCandidate(event.candidate);
      else earlyCandidates.push(event.candidate);
    };
    peer.ontrack = (event) => {
      if (peerConnectionRef.current !== peer) return;
      const stream = event.streams[0] ?? new MediaStream([event.track]);
      remoteStreamRef.current = stream;
      const video = remoteVideoRef.current;
      if (video) {
        video.srcObject = stream;
        void video.play().catch((cause: unknown) => {
          if (peerConnectionRef.current !== peer) return;
          const detail = cause instanceof Error ? cause.message : String(cause);
          setConnectionError(`无法播放远程视频流：${detail}`);
        });
      }
    };
    peer.onconnectionstatechange = () => {
      if (peerConnectionRef.current !== peer) return;
      if (peer.connectionState === "connected") {
        setConnectionStatus("connected");
        window.setTimeout(() => viewerRef.current?.focus(), 0);
      } else if (peer.connectionState === "failed") {
        setConnectionStatus("error");
        setConnectionError("WebRTC 媒体连接失败。请检查两台设备间的 UDP 防火墙设置。");
      } else if (peer.connectionState === "disconnected") {
        setConnectionStatus("error");
        setConnectionError("WebRTC 媒体连接中断。请检查局域网和 UDP 防火墙设置。");
      }
    };

    try {
      const offer = await peer.createOffer();
      await peer.setLocalDescription(offer);
      if (socket.readyState !== WebSocket.OPEN || !peer.localDescription?.sdp) {
        throw new Error("信令连接已关闭或本地 SDP 为空。");
      }
      socket.send(JSON.stringify({ type: "offer", sdp: peer.localDescription.sdp }));
      offerSent = true;
      earlyCandidates.forEach(sendCandidate);
    } catch (cause) {
      if (connectionRef.current !== socket || peerConnectionRef.current !== peer) return;
      const detail = cause instanceof Error ? cause.message : String(cause);
      setConnectionStatus("error");
      setConnectionError(`无法启动 WebRTC 连接：${detail}`);
      socket.close();
    }
  }

  function disconnectFromDevice() {
    const socket = connectionRef.current;
    connectionRef.current = null;
    if (socket) {
      socket.onerror = null;
      socket.onmessage = null;
      socket.onclose = null;
      socket.close();
    }
    const peer = peerConnectionRef.current;
    if (peer) {
      peer.onconnectionstatechange = null;
      peer.onicecandidate = null;
      peer.ontrack = null;
      peer.close();
    }
    controlChannelRef.current?.close();
    controlChannelRef.current = null;
    peerConnectionRef.current = null;
    remoteStreamRef.current = null;
    pendingRemoteCandidatesRef.current = [];
    pendingFileSendsRef.current.clear();
    clipboardSessionAllowedRef.current = false;
    setPendingClipboardOffer(null);
    if (remoteVideoRef.current) remoteVideoRef.current.srcObject = null;
    setConnectionDeviceId(null);
    setConnectionStatus("idle");
    setConnectionError("");
    setScreenSize({ width: 0, height: 0 });
    setEncoderStatus("");
  }

  async function sendClipboard() {
    const socket = connectionRef.current;
    if (!clipboardEnabled) {
      setTransferStatus("请先在设置中启用剪贴板共享。");
      return;
    }
    if (!socket || socket.readyState !== WebSocket.OPEN) {
      setTransferStatus("请先连接到远程设备。");
      return;
    }
    try {
      const text = await navigator.clipboard.readText();
      if (new TextEncoder().encode(text).byteLength > 48 * 1024) {
        setTransferStatus("剪贴板内容超过 48 KiB，未发送。");
        return;
      }
      socket.send(JSON.stringify({ type: "clipboard_text", text }));
      setTransferStatus("已发送剪贴板内容，等待对端确认。");
    } catch (cause) {
      const detail = cause instanceof Error ? cause.message : String(cause);
      setTransferStatus(`无法读取剪贴板：${detail}`);
    }
  }

  async function answerClipboardOffer(accepted: boolean) {
    const offer = pendingClipboardOffer;
    setPendingClipboardOffer(null);
    if (!accepted || !offer) return;
    try {
      await navigator.clipboard.writeText(offer.text);
      clipboardSessionAllowedRef.current = true;
      setTransferStatus("已将文本写入本机剪贴板；本次连接后续剪贴板接收将自动允许。");
    } catch (cause) {
      const detail = cause instanceof Error ? cause.message : String(cause);
      setTransferStatus(`无法写入剪贴板：${detail}`);
    }
  }

  async function answerFileOffer(accepted: boolean) {
    const offer = pendingFileOffer;
    setPendingFileOffer(null);
    if (!offer) return;
    try {
      await invoke("answer_file_transfer", {
        transferId: offer.transferId,
        accepted,
      });
      setTransferStatus(accepted ? `正在接收 ${offer.fileName}…` : `已拒绝接收 ${offer.fileName}。`);
    } catch (cause) {
      const detail = cause instanceof Error ? cause.message : String(cause);
      setTransferStatus(`无法处理文件请求：${detail}`);
    }
  }

  async function selectFile(file: File | undefined) {
    if (!file) return;
    const socket = connectionRef.current;
    if (!socket || socket.readyState !== WebSocket.OPEN) {
      setTransferStatus("请先连接到远程设备。");
      return;
    }
    if (file.size === 0 || file.size > 100 * 1024 * 1024) {
      setTransferStatus("文件必须大于 0 且不超过 100 MiB。");
      return;
    }
    const transferId = crypto.randomUUID();
    pendingFileSendsRef.current.set(transferId, file);
    socket.send(JSON.stringify({
      type: "file_offer",
      transfer_id: transferId,
      file_name: file.name,
      size: file.size,
    }));
    setTransferStatus(`已请求发送 ${file.name}，等待对端批准。`);
  }

  async function transmitFile(socket: WebSocket, transferId: string, file: File) {
    try {
      const chunkSize = 48 * 1024;
      for (let offset = 0; offset < file.size; offset += chunkSize) {
        while (socket.bufferedAmount > 512 * 1024) {
          if (socket.readyState !== WebSocket.OPEN) throw new Error("连接已关闭。");
          await new Promise((resolve) => window.setTimeout(resolve, 30));
        }
        const chunk = await file.slice(offset, offset + chunkSize).arrayBuffer();
        if (socket.readyState !== WebSocket.OPEN) throw new Error("连接已关闭。");
        socket.send(chunk);
        setTransferStatus(`正在发送 ${file.name}：${Math.min(offset + chunk.byteLength, file.size)} / ${file.size} 字节`);
      }
      socket.send(JSON.stringify({ type: "file_complete", transfer_id: transferId }));
      pendingFileSendsRef.current.delete(transferId);
    } catch (cause) {
      pendingFileSendsRef.current.delete(transferId);
      const detail = cause instanceof Error ? cause.message : String(cause);
      setTransferStatus(`文件发送失败：${detail}`);
    }
  }

  function sendInput(message: Record<string, string | number | boolean>) {
    const channel = controlChannelRef.current;
    if (channel?.readyState === "open" && channel.bufferedAmount < 512_000) {
      channel.send(JSON.stringify(message));
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
          {showLocalInfo && (
            <div className="sidebar-info-popover" role="dialog" aria-label="本机信息">
              <strong>本机信息</strong>
              {isLoadingLocalInfo ? (
                <p>正在读取设备信息…</p>
              ) : localInfoError ? (
                <p className="popover-error">{localInfoError}</p>
              ) : localDeviceInfo ? (
                <>
                  <span className="popover-label">设备名称</span>
                  <code>{localDeviceInfo.name}</code>
                  <span className="popover-label">局域网地址</span>
                  <div className="popover-address">
                    <code>{localDeviceInfo.address}</code>
                    <button
                      className="icon-button small"
                      type="button"
                      aria-label="复制本机地址"
                      title="复制地址"
                      onClick={() => void copyLocalAddress()}
                    >
                      <Icon name="copy" />
                    </button>
                  </div>
                  {copyAddressStatus && <span className="popover-copy-status">{copyAddressStatus}</span>}
                </>
              ) : (
                <p>请在 Lantern 桌面应用中查看本机网络信息。</p>
              )}
            </div>
          )}
          <div className="sidebar-bottom-actions">
            <button
              className="profile profile-button"
              type="button"
              aria-expanded={showLocalInfo}
              onClick={() => void toggleLocalInfo()}
            >
              <span className="avatar">L</span>
              <span className="profile-copy">
                <strong>本机工作区</strong>
                <span>点击查看设备信息</span>
              </span>
              <Icon name="chevron" />
            </button>
            <button
              className="icon-button settings-button"
              type="button"
              aria-label="应用设置"
              title="应用设置"
              onClick={() => void openSettings()}
            >
              <Icon name="settings" />
            </button>
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
            <div className="layout-toggle" role="group" aria-label="设备布局">
              {(["tile", "list", "grid"] as const).map((layout) => (
                <button
                  className={deviceLayout === layout ? "layout-option selected" : "layout-option"}
                  key={layout}
                  type="button"
                  aria-pressed={deviceLayout === layout}
                  title={layout === "tile" ? "平铺视图" : layout === "list" ? "列表视图" : "网格视图"}
                  onClick={() => setDeviceLayout(layout)}
                >
                  <Icon name={layout === "list" ? "list" : layout === "tile" ? "tile" : "grid"} />
                  <span>{layout === "tile" ? "平铺" : layout === "list" ? "列表" : "网格"}</span>
                </button>
              ))}
            </div>
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
            <div className={`device-grid layout-${deviceLayout}`}>
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
                      <video
                        autoPlay
                        className="remote-video"
                        muted
                        playsInline
                        ref={remoteVideoRef}
                        onLoadedMetadata={(event) => setScreenSize({
                          width: event.currentTarget.videoWidth,
                          height: event.currentTarget.videoHeight,
                        })}
                      />
                      <span className="remote-live">
                        <span className="lan-status-dot" />
                        WebRTC · {encoderStatus || "编码器检测中"} · 点击后键鼠可控
                      </span>
                      <div
                        className="remote-toolbar"
                        onMouseMove={(event) => event.stopPropagation()}
                        onMouseUp={(event) => event.stopPropagation()}
                      >
                        <button
                          className="icon-button remote-toolbar-button"
                          type="button"
                          aria-label="发送剪贴板"
                          title="发送本机剪贴板"
                          onMouseDown={(event) => {
                            event.preventDefault();
                            event.stopPropagation();
                          }}
                          onClick={() => void sendClipboard()}
                        >
                          <Icon name="copy" />
                        </button>
                        <button
                          className="icon-button remote-toolbar-button"
                          type="button"
                          aria-label="发送文件"
                          title="发送文件"
                          onMouseDown={(event) => {
                            event.preventDefault();
                            event.stopPropagation();
                          }}
                          onClick={() => fileInputRef.current?.click()}
                        >
                          <Icon name="file" />
                        </button>
                        <button
                          className="icon-button remote-toolbar-button"
                          type="button"
                          aria-label={isRemoteFullscreen ? "退出全屏" : "全屏显示"}
                          title={isRemoteFullscreen ? "退出全屏" : "全屏显示"}
                          onMouseDown={(event) => {
                            event.preventDefault();
                            event.stopPropagation();
                          }}
                          onClick={() => void toggleRemoteFullscreen()}
                        >
                          <Icon name={isRemoteFullscreen ? "minimize" : "maximize"} />
                        </button>
                      </div>
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
                  <input
                    ref={fileInputRef}
                    className="visually-hidden"
                    type="file"
                    onChange={(event) => {
                      void selectFile(event.currentTarget.files?.[0]);
                      event.currentTarget.value = "";
                    }}
                  />
                  {transferStatus && <div className="transfer-status" role="status">{transferStatus}</div>}
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
                    <div
                      className="device-menu-wrap"
                      onBlur={(event) => {
                        if (!event.currentTarget.contains(event.relatedTarget as Node | null)) {
                          setOpenDeviceMenuId(null);
                        }
                      }}
                    >
                      <button
                        className="icon-button card-menu"
                        type="button"
                        aria-label={`${device.name} 的更多操作`}
                        title="更多操作"
                        aria-haspopup="menu"
                        aria-expanded={openDeviceMenuId === device.id}
                        onClick={() => setOpenDeviceMenuId((current) =>
                          current === device.id ? null : device.id,
                        )}
                      >
                        <Icon name="dots" />
                      </button>
                      {openDeviceMenuId === device.id && (
                        <div className="device-menu" role="menu">
                          <button
                            type="button"
                            role="menuitem"
                            onClick={() => {
                              setOpenDeviceMenuId(null);
                              setPendingRemoval(device);
                            }}
                          >
                            移除设备…
                          </button>
                        </div>
                      )}
                    </div>
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
              placeholder="192.168.1.24"
              value={address}
            />
            <span className="field-hint">端口可省略，默认使用通信端口 {DEFAULT_PORT}。</span>
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
      {showSettings && (
        <div
          className="modal-backdrop"
          onMouseDown={(event) => {
            if (event.target === event.currentTarget) setShowSettings(false);
          }}
        >
          <section className="device-dialog settings-dialog" aria-labelledby="settings-title">
            <button
              className="dialog-close icon-button"
              type="button"
              aria-label="关闭"
              onClick={() => setShowSettings(false)}
            >
              <Icon name="close" />
            </button>
            <span className="dialog-icon"><Icon name="settings" /></span>
            <h2 id="settings-title">应用设置</h2>
            <p>画质偏好会在下一次连接时应用到被控端。</p>
            <fieldset className="quality-options">
              <legend>远程画面偏好</legend>
              <label className={videoQuality === "quality" ? "quality-option selected" : "quality-option"}>
                <input
                  checked={videoQuality === "quality"}
                  name="video-quality"
                  onChange={() => setVideoQuality("quality")}
                  type="radio"
                  value="quality"
                />
                <span>
                  <strong>画质优先</strong>
                  <small>更高分辨率和码率，可能增加带宽与编码负载。</small>
                </span>
              </label>
              <label className={videoQuality === "balanced" ? "quality-option selected" : "quality-option"}>
                <input
                  checked={videoQuality === "balanced"}
                  name="video-quality"
                  onChange={() => setVideoQuality("balanced")}
                  type="radio"
                  value="balanced"
                />
                <span>
                  <strong>平衡</strong>
                  <small>默认画面尺寸与帧率，兼顾清晰度和响应。</small>
                </span>
              </label>
              <label className={videoQuality === "smooth" ? "quality-option selected" : "quality-option"}>
                <input
                  checked={videoQuality === "smooth"}
                  name="video-quality"
                  onChange={() => setVideoQuality("smooth")}
                  type="radio"
                  value="smooth"
                />
                <span>
                  <strong>流畅度优先</strong>
                  <small>提高目标帧率并降低分辨率上限，适合动作较多的画面。</small>
                </span>
              </label>
            </fieldset>
            <label className="clipboard-setting">
              <input
                checked={clipboardEnabled}
                onChange={(event) => setClipboardEnabled(event.currentTarget.checked)}
                type="checkbox"
              />
              <span>
                <strong>启用手动剪贴板共享</strong>
                <small>控制端需主动发送；接收端首次确认后，本次连接内的后续文本将自动写入，断开后重新询问。</small>
              </span>
            </label>
            <section className="trusted-devices-section">
              <h3>信任设备</h3>
              <p>信任后，同一 IP 的连接请求将自动获批。仅信任你控制的设备。</p>
              {trustedDevicesError && <p className="form-error" role="alert">{trustedDevicesError}</p>}
              {trustedDevices.length === 0 ? (
                <p className="trusted-empty">没有已信任的设备。</p>
              ) : (
                <ul className="trusted-device-list">
                  {trustedDevices.map((device) => (
                    <li key={device.ipAddress}>
                      <code>{device.ipAddress}</code>
                      <button
                        className="text-button"
                        type="button"
                        onClick={() => void removeTrustedDevice(device.ipAddress)}
                      >
                        移除
                      </button>
                    </li>
                  ))}
                </ul>
              )}
            </section>
            <div className="dialog-actions">
              <button
                className="primary-button"
                onClick={() => setShowSettings(false)}
                type="button"
              >
                完成
              </button>
            </div>
          </section>
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
            <label className="trust-device-choice">
              <input
                checked={trustCurrentDevice}
                onChange={(event) => setTrustCurrentDevice(event.currentTarget.checked)}
                type="checkbox"
              />
              <span>信任此控制端 IP，以后自动批准来自该 IP 的连接</span>
            </label>
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
      {pendingClipboardOffer && (
        <div className="modal-backdrop">
          <section className="device-dialog approval-dialog" aria-labelledby="clipboard-offer-title">
            <span className="dialog-icon"><Icon name="copy" /></span>
            <h2 id="clipboard-offer-title">接收远程剪贴板？</h2>
            <p className="clipboard-preview">{pendingClipboardOffer.text}</p>
            <div className="dialog-actions">
              <button className="secondary-button" onClick={() => void answerClipboardOffer(false)} type="button">拒绝</button>
              <button className="primary-button" onClick={() => void answerClipboardOffer(true)} type="button">本次连接允许</button>
            </div>
          </section>
        </div>
      )}
      {pendingFileOffer && (
        <div className="modal-backdrop">
          <section className="device-dialog approval-dialog" aria-labelledby="file-offer-title">
            <span className="dialog-icon"><Icon name="file" /></span>
            <h2 id="file-offer-title">接收文件？</h2>
            <p>
              对端请求发送 <strong>{pendingFileOffer.fileName}</strong>（{(pendingFileOffer.size / 1024 / 1024).toFixed(2)} MiB）。
              接受后将保存到“下载/Lantern Received”目录。
            </p>
            <div className="dialog-actions">
              <button className="secondary-button" onClick={() => void answerFileOffer(false)} type="button">拒绝</button>
              <button className="primary-button" onClick={() => void answerFileOffer(true)} type="button">接受文件</button>
            </div>
          </section>
        </div>
      )}
      {pendingRemoval && (
        <div className="modal-backdrop">
          <section className="device-dialog approval-dialog" aria-labelledby="remove-device-title">
            <span className="dialog-icon"><Icon name="info" /></span>
            <h2 id="remove-device-title">移除已添加的设备？</h2>
            <p>
              将从本机设备列表中移除“{pendingRemoval.name}”（{pendingRemoval.address}）。
              此操作不会影响远程电脑。
            </p>
            <div className="dialog-actions">
              <button
                className="secondary-button"
                onClick={() => setPendingRemoval(null)}
                type="button"
              >
                取消
              </button>
              <button
                className="danger-button"
                onClick={() => {
                  removeDevice(pendingRemoval.id);
                  setPendingRemoval(null);
                }}
                type="button"
              >
                确认移除
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
    tile: <><rect x="3" y="4" width="8" height="7" rx="1" /><rect x="13" y="4" width="8" height="7" rx="1" /><rect x="3" y="13" width="8" height="7" rx="1" /><rect x="13" y="13" width="8" height="7" rx="1" /></>,
    list: <><path d="M9 6h11M9 12h11M9 18h11" /><path d="M4 6h.01M4 12h.01M4 18h.01" /></>,
    file: <><path d="M6 3h8l4 4v14H6z" /><path d="M14 3v5h5M9 13h6m-6 4h6" /></>,
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
    maximize: <><path d="M8 4H4v4m12-4h4v4M4 16v4h4m12-4v4h-4" /></>,
    minimize: <><path d="M8 4v4H4m12-4v4h4M4 16h4v4m12-4h-4v4" /></>,
    copy: <><rect x="8" y="8" width="12" height="12" rx="2" /><path d="M16 8V5a1 1 0 0 0-1-1H5a1 1 0 0 0-1 1v10a1 1 0 0 0 1 1h3" /></>,
    settings: <><circle cx="12" cy="12" r="3" /><path d="m19.4 15 .1.1 1.2 2.1-2 2-2.2-1.1a8 8 0 0 1-2 .8L14 21h-4l-.5-2.2a8 8 0 0 1-2-.8L5.3 19l-2-2 1.1-2.2a8 8 0 0 1-.8-2L1.5 12l2.1-2.8.1-.2.1-.2a8 8 0 0 1 .8-2L3.5 4.7l2-2 2.2 1.1a8 8 0 0 1 2-.8L10 1h4l.5 2.2a8 8 0 0 1 2 .8l2.2-1.1 2 2-1.1 2.2a8 8 0 0 1 .8 2l2.1 1.1-2.1 2.8-.1.2-.1.2a8 8 0 0 1-.8 2Z" transform="translate(1 1) scale(.92)" /></>,
  };

  return (
    <svg aria-hidden="true" {...common}>
      {paths[name] ?? null}
    </svg>
  );
}

export default App;
