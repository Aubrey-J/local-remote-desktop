import { useEffect, useMemo, useRef, useState } from "react";
import { invoke, isTauri } from "@tauri-apps/api/core";
import { emit, listen } from "@tauri-apps/api/event";
import { WebviewWindow } from "@tauri-apps/api/webviewWindow";
import { getCurrentWindow, UserAttentionType } from "@tauri-apps/api/window";
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

type AvailableDisplay = {
  id: number;
  name: string;
  width: number;
  height: number;
  primary: boolean;
};

type DiscoveredDevice = { name: string; address: string };

type TrustedDevice = { ipAddress: string };
type DeviceLayout = "grid" | "tile" | "list";
type FileOffer = {
  transferId: string;
  fileName: string;
  size: number;
  sha256: string;
  receivedBytes: number;
  direction: "to-agent" | "from-agent";
};
type ActiveFileTransfer = {
  transferId: string;
  fileName: string;
  size: number;
  transferred: number;
  paused: boolean;
  path: "websocket" | "tauri";
  mode: "send" | "receive";
};
type PendingFileIdentity = { fingerprint: string; transferId: string; updatedAt: number };
type ClipboardOffer = { text: string };
type IncomingFileTransfer = {
  transferId: string;
  fileName: string;
  size: number;
  receivedBytes: number;
  writeQueue: Promise<void>;
  writeError: string | null;
};
type KeyboardLock = { lock: (keys?: string[]) => Promise<void>; unlock: () => void };

type RemoteStatus = "idle" | "connecting" | "waitingApproval" | "connected" | "error";
type StandaloneWindowStatus = RemoteStatus | "opening";
type SettingsSection = "video" | "audio" | "clipboard" | "window" | "trusted" | "diagnostics";
type DiagnosticEvent =
  | "discovery_started"
  | "discovery_completed"
  | "discovery_failed"
  | "connection_started"
  | "connection_approved"
  | "connection_established"
  | "connection_failed"
  | "connection_interrupted"
  | "connection_closed"
  | "agent_started"
  | "agent_stopped"
  | "agent_start_failed"
  | "agent_stop_failed"
  | "agent_connection_approved"
  | "agent_connection_established"
  | "agent_connection_failed"
  | "agent_connection_interrupted"
  | "agent_connection_closed"
  | "file_transfer_started"
  | "file_transfer_offer_received"
  | "file_transfer_accepted"
  | "file_transfer_rejected"
  | "file_transfer_paused"
  | "file_transfer_resumed"
  | "file_transfer_cancelled"
  | "file_transfer_progress"
  | "file_transfer_interrupted"
  | "file_transfer_completed"
  | "file_transfer_failed"
  | "clipboard_sync_failed"
  | "clipboard_text_sent"
  | "clipboard_text_received"
  | "audio_inbound_rtp_stats"
  | "audio_inbound_stats_read_failed"
  | "audio_remote_track_received"
  | "audio_remote_track_unmuted"
  | "audio_playback_started"
  | "audio_playback_blocked"
  | "audio_playback_failed"
  | "quality_reduced"
  | "quality_restored";

const STORAGE_KEY = "lantern.devices";
const QUALITY_STORAGE_KEY = "lantern.video-quality";
const LAYOUT_STORAGE_KEY = "lantern.device-layout";
const CLIPBOARD_STORAGE_KEY = "lantern.clipboard-enabled";
const SYSTEM_AUDIO_PLAY_STORAGE_KEY = "lantern.play-remote-system-audio";
const SYSTEM_AUDIO_SHARE_STORAGE_KEY = "lantern.share-system-audio";
const CAPTURE_DISPLAY_STORAGE_KEY = "lantern.capture-display";
const DEFAULT_PORT = "4200";
const FILE_TRANSFER_IDENTITIES_KEY = "lantern.file-transfer-identities";

function readDeviceLayout(): DeviceLayout {
  const value = localStorage.getItem(LAYOUT_STORAGE_KEY);
  return value === "tile" || value === "list" ? value : "grid";
}

function readPendingFileIdentities(): PendingFileIdentity[] {
  const raw = localStorage.getItem(FILE_TRANSFER_IDENTITIES_KEY);
  if (!raw) return [];
  const parsed: unknown = JSON.parse(raw);
  if (
    !Array.isArray(parsed) ||
    parsed.some((item) =>
      typeof item !== "object" ||
      item === null ||
      typeof item.fingerprint !== "string" ||
      !/^[a-f0-9]{64}$/.test(item.fingerprint) ||
      typeof item.transferId !== "string" ||
      typeof item.updatedAt !== "number"
    )
  ) {
    throw new Error("本机文件续传记录格式无效。");
  }
  const expiration = Date.now() - 30 * 24 * 60 * 60 * 1000;
  return parsed.filter((item: PendingFileIdentity) => item.updatedAt >= expiration);
}

function rememberFileTransfer(fingerprint: string): string {
  const identities = readPendingFileIdentities().filter((item) => item.fingerprint !== fingerprint);
  const transferId = crypto.randomUUID();
  identities.push({ fingerprint, transferId, updatedAt: Date.now() });
  localStorage.setItem(FILE_TRANSFER_IDENTITIES_KEY, JSON.stringify(identities));
  return transferId;
}

async function getOrCreateFileTransferId(
  fileName: string,
  sha256: string,
  targetIdentity: string,
): Promise<string> {
  const identityDigest = await crypto.subtle.digest(
    "SHA-256",
    new TextEncoder().encode(`${targetIdentity}\0${fileName}\0${sha256}`),
  );
  const fingerprint = Array.from(new Uint8Array(identityDigest), (byte) =>
    byte.toString(16).padStart(2, "0"),
  ).join("");
  const identities = readPendingFileIdentities();
  const existing = identities.find((item) => item.fingerprint === fingerprint);
  if (existing) {
    existing.updatedAt = Date.now();
    localStorage.setItem(FILE_TRANSFER_IDENTITIES_KEY, JSON.stringify(identities));
    return existing.transferId;
  }
  return rememberFileTransfer(fingerprint);
}

function forgetFileTransfer(transferId: string) {
  const identities = readPendingFileIdentities().filter((item) => item.transferId !== transferId);
  localStorage.setItem(FILE_TRANSFER_IDENTITIES_KEY, JSON.stringify(identities));
}

async function hashFile(file: File): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", await file.arrayBuffer());
  return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join("");
}
function readClipboardEnabled() {
  return localStorage.getItem(CLIPBOARD_STORAGE_KEY) === "true";
}

function readSystemAudioPreference(key: string) {
  return localStorage.getItem(key) === "true";
}

function readCaptureDisplayId(): number | null {
  const value = localStorage.getItem(CAPTURE_DISPLAY_STORAGE_KEY);
  if (value === null) return null;
  const displayId = Number(value);
  return Number.isSafeInteger(displayId) && displayId >= 0 ? displayId : null;
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
  const standaloneDeviceId = new URLSearchParams(window.location.search).get("remoteDevice");
  const [savedState] = useState(readDevices);
  const [devices, setDevices] = useState<Device[]>(savedState.devices);
  const [storageError, setStorageError] = useState(savedState.error);
  const [activeId, setActiveId] = useState<string | null>(null);
  const [isAdding, setIsAdding] = useState(false);
  const [editingDevice, setEditingDevice] = useState<Device | null>(null);
  const [discoveredDevices, setDiscoveredDevices] = useState<DiscoveredDevice[]>([]);
  const [discoveryStatus, setDiscoveryStatus] = useState("");
  const [isDiscovering, setIsDiscovering] = useState(false);
  const [isSavingDevice, setIsSavingDevice] = useState(false);
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
  const [settingsSection, setSettingsSection] = useState<SettingsSection>("video");
  const [diagnosticLog, setDiagnosticLog] = useState("");
  const [diagnosticLogError, setDiagnosticLogError] = useState("");
  const [availableDisplays, setAvailableDisplays] = useState<AvailableDisplay[]>([]);
  const [selectedDisplayId, setSelectedDisplayId] = useState<number | null>(() => {
    try {
      return readCaptureDisplayId();
    } catch {
      return null;
    }
  });
  const [displaySettingsError, setDisplaySettingsError] = useState("");
  const [closeToTray, setCloseToTray] = useState(true);
  const [closeToTrayError, setCloseToTrayError] = useState("");
  const [savingCloseToTray, setSavingCloseToTray] = useState(false);
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
  const [playRemoteSystemAudio, setPlayRemoteSystemAudio] = useState(() => {
    try {
      return readSystemAudioPreference(SYSTEM_AUDIO_PLAY_STORAGE_KEY);
    } catch {
      return false;
    }
  });
  const [shareSystemAudio, setShareSystemAudio] = useState(() => {
    try {
      return readSystemAudioPreference(SYSTEM_AUDIO_SHARE_STORAGE_KEY);
    } catch {
      return false;
    }
  });
  const [audioStatus, setAudioStatus] = useState("");
  const [audioTransportStatus, setAudioTransportStatus] = useState("");
  const [audioPlaybackBlocked, setAudioPlaybackBlocked] = useState(false);
  const [trustedDevices, setTrustedDevices] = useState<TrustedDevice[]>([]);
  const [trustedDevicesError, setTrustedDevicesError] = useState("");
  const [trustCurrentDevice, setTrustCurrentDevice] = useState(false);
  const [pendingFileOffer, setPendingFileOffer] = useState<FileOffer | null>(null);
  const [pendingClipboardOffer, setPendingClipboardOffer] = useState<ClipboardOffer | null>(null);
  const [activeFileTransfer, setActiveFileTransfer] = useState<ActiveFileTransfer | null>(null);
  const [transferStatus, setTransferStatus] = useState("");
  const fileInputRef = useRef<HTMLInputElement>(null);
  const agentFileInputRef = useRef<HTMLInputElement>(null);
  const incomingFileTransferRef = useRef<IncomingFileTransfer | null>(null);
  const pendingAgentFileRef = useRef<{
    transferId: string;
    file: File;
    sha256: string;
    paused: boolean;
    cancelled: boolean;
  } | null>(null);
  const agentPeerIdentityRef = useRef("");
  const lastClipboardTextRef = useRef<string | null>(null);
  const clipboardReadErrorReportedRef = useRef(false);

  function updateActiveFileTransfer(next: ActiveFileTransfer | null) {
    activeFileTransferRef.current = next;
    setActiveFileTransfer(next);
  }

  function clearFileTransferIdentity(transferId: string) {
    try {
      forgetFileTransfer(transferId);
    } catch (cause) {
      const detail = cause instanceof Error ? cause.message : String(cause);
      setTransferStatus(`无法更新本机续传记录：${detail}`);
    }
  }
  const [name, setName] = useState("");
  const [address, setAddress] = useState("");
  const [error, setError] = useState("");
  const hasMounted = useRef(false);
  const connectionRef = useRef<WebSocket | null>(null);
  const peerConnectionRef = useRef<RTCPeerConnection | null>(null);
  const connectionTimeoutRef = useRef<number | null>(null);
  const connectionRecoveryTimerRef = useRef<number | null>(null);
  const controlChannelRef = useRef<RTCDataChannel | null>(null);
  const pressedRemoteMouseButtonsRef = useRef<Set<string>>(new Set());
  const pendingFileSendsRef = useRef<Map<string, {
    file: File;
    sha256: string;
    paused: boolean;
    cancelled: boolean;
  }>>(new Map());
  const activeFileTransferRef = useRef<ActiveFileTransfer | null>(null);
  const clipboardSessionAllowedRef = useRef(false);
  const remoteStreamRef = useRef<MediaStream | null>(null);
  const pendingRemoteCandidatesRef = useRef<RTCIceCandidateInit[]>([]);
  const lastPointerMove = useRef(0);
  const viewerRef = useRef<HTMLDivElement>(null);
  const remoteVideoRef = useRef<HTMLVideoElement>(null);
  const remoteAudioRef = useRef<HTMLAudioElement>(null);
  const remoteAudioStreamRef = useRef<MediaStream | null>(null);
  const standaloneClosingRef = useRef(false);
  const [agentRunning, setAgentRunning] = useState(false);
  const [agentAddress, setAgentAddress] = useState("");
  const [agentStatus, setAgentStatus] = useState("");
  const [agentConnected, setAgentConnected] = useState(false);
  const [agentClipboardStatus, setAgentClipboardStatus] = useState("");
  const [pendingRequest, setPendingRequest] = useState<ConnectionRequest | null>(null);
  const [connectionStatus, setConnectionStatus] = useState<RemoteStatus>("idle");
  const [connectionDeviceId, setConnectionDeviceId] = useState<string | null>(null);
  const [standaloneWindowStatuses, setStandaloneWindowStatuses] = useState<Record<string, StandaloneWindowStatus>>({});
  const [connectionError, setConnectionError] = useState("");
  const [encoderStatus, setEncoderStatus] = useState("");
  const [screenSize, setScreenSize] = useState({ width: 0, height: 0 });
  const [isDomFullscreen, setIsDomFullscreen] = useState(false);
  const [isNativeFullscreen, setIsNativeFullscreen] = useState(false);
  const isRemoteFullscreen = isDomFullscreen || isNativeFullscreen;
  const [keyboardLockWarning, setKeyboardLockWarning] = useState("");
  const standaloneConnectionStarted = useRef(false);
  const isMainWindow = !isTauri() || getCurrentWindow().label === "main";

  useEffect(() => {
    if (!isTauri()) return;
    void invoke<boolean>("get_close_to_tray")
      .then(setCloseToTray)
      .catch((cause: unknown) => {
        const detail = cause instanceof Error ? cause.message : String(cause);
        setCloseToTrayError(`无法读取关闭窗口设置：${detail}`);
      });
  }, []);

  useEffect(() => {
    if (!isTauri()) return;
    let cancelled = false;
    let unlisten: (() => void) | undefined;
    void listen<{ deviceId: string; status: StandaloneWindowStatus | "closed" }>(
      "remote-window-state",
      ({ payload }) => {
        if (!isMainWindow) return;
        const { deviceId, status } = payload;
        if (status === "closed") {
          setStandaloneWindowStatuses((current) => {
            const { [deviceId]: _closed, ...remaining } = current;
            return remaining;
          });
          return;
        }
        setStandaloneWindowStatuses((current) => ({ ...current, [deviceId]: status }));
      },
    ).then((stopListening) => {
      if (cancelled) stopListening();
      else unlisten = stopListening;
    }).catch((cause: unknown) => {
      const detail = cause instanceof Error ? cause.message : String(cause);
      setTransferStatus(`无法同步独立控制窗口状态：${detail}`);
    });
    return () => {
      cancelled = true;
      unlisten?.();
    };
  }, []);

  useEffect(() => {
    if (!isTauri() || !standaloneDeviceId || standaloneClosingRef.current) return;
    void emit("remote-window-state", { deviceId: standaloneDeviceId, status: connectionStatus })
      .catch((cause: unknown) => {
        const detail = cause instanceof Error ? cause.message : String(cause);
        setConnectionError(`无法同步独立控制窗口状态：${detail}`);
      });
  }, [connectionStatus, standaloneDeviceId]);

  useEffect(() => {
    if (!isTauri() || !standaloneDeviceId) return;
    let cancelled = false;
    let unlisten: (() => void) | undefined;
    void listen<{ deviceId: string }>("remote-window-status-request", ({ payload }) => {
      if (payload.deviceId !== standaloneDeviceId) return;
      void emit("remote-window-state", { deviceId: standaloneDeviceId, status: connectionStatus })
        .catch((cause: unknown) => {
          const detail = cause instanceof Error ? cause.message : String(cause);
          setConnectionError(`无法同步独立控制窗口状态：${detail}`);
        });
    }).then((stopListening) => {
      if (cancelled) stopListening();
      else unlisten = stopListening;
    }).catch((cause: unknown) => {
      const detail = cause instanceof Error ? cause.message : String(cause);
      setConnectionError(`无法监听独立窗口状态请求：${detail}`);
    });
    return () => {
      cancelled = true;
      unlisten?.();
    };
  }, [connectionStatus, standaloneDeviceId]);

  useEffect(() => {
    if (!isTauri() || !standaloneDeviceId) return;
    let cancelled = false;
    let unlistenCloseEvent: (() => void) | undefined;
    void getCurrentWindow().onCloseRequested(() => {
      standaloneClosingRef.current = true;
      disconnectFromDevice(false);
      void emit("remote-window-state", { deviceId: standaloneDeviceId, status: "closed" })
        .catch((cause: unknown) => {
          const detail = cause instanceof Error ? cause.message : String(cause);
          setConnectionError(`无法同步独立控制窗口关闭状态：${detail}`);
        });
    }).then((stopListening) => {
      if (cancelled) stopListening();
      else unlistenCloseEvent = stopListening;
    }).catch((cause: unknown) => {
      const detail = cause instanceof Error ? cause.message : String(cause);
      setConnectionError(`无法监听独立窗口关闭状态：${detail}`);
    });
    return () => {
      cancelled = true;
      unlistenCloseEvent?.();
    };
  }, [standaloneDeviceId]);

  useEffect(() => {
    const className = "native-fullscreen-document";
    if (isNativeFullscreen) {
      document.documentElement.classList.add(className);
    } else {
      document.documentElement.classList.remove(className);
    }
    return () => document.documentElement.classList.remove(className);
  }, [isNativeFullscreen]);

  useEffect(() => {
    if (!standaloneDeviceId || standaloneConnectionStarted.current) return;
    const device = devices.find((item) => item.id === standaloneDeviceId);
    if (!device) return;
    standaloneConnectionStarted.current = true;
    setActiveId(device.id);
    connectToDevice(device);
  }, [devices, standaloneDeviceId]);

  useEffect(() => {
    if (!isTauri() || !clipboardEnabled || connectionStatus !== "connected") {
      lastClipboardTextRef.current = null;
      return;
    }
    let cancelled = false;
    const pollClipboard = async () => {
      try {
        const text = await invoke<string>("get_system_clipboard_text");
        clipboardReadErrorReportedRef.current = false;
        if (cancelled) return;
        const previous = lastClipboardTextRef.current;
        if (previous === null) {
          lastClipboardTextRef.current = text;
          return;
        }
        if (text === previous) return;
        const socket = connectionRef.current;
        if (!socket || socket.readyState !== WebSocket.OPEN) return;
        if (new TextEncoder().encode(text).byteLength > 48 * 1024) {
          setTransferStatus("剪贴板内容超过 48 KiB，未同步。");
          lastClipboardTextRef.current = text;
          return;
        }
        socket.send(JSON.stringify({ type: "clipboard_text", text }));
        recordDiagnosticEvent("clipboard_text_sent", new TextEncoder().encode(text).byteLength);
        lastClipboardTextRef.current = text;
      } catch (cause) {
        if (cancelled || clipboardReadErrorReportedRef.current) return;
        clipboardReadErrorReportedRef.current = true;
        const detail = cause instanceof Error ? cause.message : String(cause);
        setTransferStatus(`无法监视本机剪贴板：${detail}`);
      }
    };
    void pollClipboard();
    const timer = window.setInterval(() => void pollClipboard(), 500);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [clipboardEnabled, connectionStatus]);

  useEffect(() => {
    if (connectionStatus !== "connected") return;
    let cancelled = false;
    let previousReceived: number | null = null;
    let previousLost: number | null = null;
    let poorSamples = 0;
    let goodSamples = 0;
    let statsReadErrorReported = false;
    let level: "normal" | "constrained" = "normal";
    const sampleNetwork = async () => {
      const peer = peerConnectionRef.current;
      const socket = connectionRef.current;
      if (!peer || peer.connectionState !== "connected" || !socket || socket.readyState !== WebSocket.OPEN) return;
      try {
        const stats = await peer.getStats();
        if (cancelled || peerConnectionRef.current !== peer) return;
        statsReadErrorReported = false;
        statsReadErrorReported = false;
        let received = 0;
        let lost = 0;
        let roundTripTime = 0;
        stats.forEach((stat) => {
          if (stat.type === "inbound-rtp") {
            const video = stat as RTCInboundRtpStreamStats & { kind?: string; mediaType?: string };
            if ((video.kind ?? video.mediaType) === "video") {
              received = video.packetsReceived ?? 0;
              lost = video.packetsLost ?? 0;
            }
          } else if (stat.type === "candidate-pair") {
            const pair = stat as RTCIceCandidatePairStats;
            if (pair.state === "succeeded" && pair.nominated) {
              roundTripTime = pair.currentRoundTripTime ?? 0;
            }
          }
        });
        if (previousReceived !== null && previousLost !== null) {
          const receivedDelta = Math.max(0, received - previousReceived);
          const lostDelta = Math.max(0, lost - previousLost);
          const samplePackets = receivedDelta + lostDelta;
          const lossRate = lostDelta / Math.max(1, samplePackets);
          if (roundTripTime >= 0.3 || (samplePackets > 0 && lossRate >= 0.05)) {
            poorSamples += 1;
            goodSamples = 0;
          } else if (
            samplePackets > 0 &&
            roundTripTime > 0 &&
            roundTripTime <= 0.18 &&
            lossRate <= 0.01
          ) {
            goodSamples += 1;
            poorSamples = 0;
          } else {
            poorSamples = 0;
            goodSamples = 0;
          }
          const nextLevel =
            level === "normal" && poorSamples >= 2 ? "constrained" :
              level === "constrained" && goodSamples >= 3 ? "normal" : level;
          if (nextLevel !== level) {
            level = nextLevel;
            socket.send(JSON.stringify({ type: "quality_adjust", level }));
            recordDiagnosticEvent(level === "constrained" ? "quality_reduced" : "quality_restored");
            setTransferStatus(level === "constrained"
              ? "检测到网络延迟或丢包，已降低远端视频码率。"
              : "网络状态已恢复，已还原远端视频码率。");
          }
        }
        previousReceived = received;
        previousLost = lost;
      } catch (cause) {
        if (cancelled || statsReadErrorReported) return;
        statsReadErrorReported = true;
        const detail = cause instanceof Error ? cause.message : String(cause);
        setTransferStatus(`无法读取网络质量统计：${detail}`);
      }
    };
    void sampleNetwork();
    const timer = window.setInterval(() => void sampleNetwork(), 3000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [connectionStatus]);

  useEffect(() => {
    if (connectionStatus !== "connected" || !playRemoteSystemAudio) {
      setAudioTransportStatus("");
      return;
    }
    let cancelled = false;
    let lastStatsLogAt = 0;
    let statsReadErrorReported = false;
    const sampleAudio = async () => {
      const peer = peerConnectionRef.current;
      if (!peer || peer.connectionState !== "connected") return;
      try {
        const stats = await peer.getStats();
        if (cancelled || peerConnectionRef.current !== peer) return;
        let receivedAudio = false;
        let packetsReceived = 0;
        let bytesReceived = 0;
        let audioLevel = 0;
        stats.forEach((stat) => {
          if (stat.type !== "inbound-rtp") return;
          const inbound = stat as RTCInboundRtpStreamStats & {
            kind?: string;
            mediaType?: string;
            audioLevel?: number;
          };
          if ((inbound.kind ?? inbound.mediaType) !== "audio") return;
          receivedAudio = true;
          packetsReceived += inbound.packetsReceived ?? 0;
          bytesReceived += inbound.bytesReceived ?? 0;
          audioLevel = Math.max(audioLevel, inbound.audioLevel ?? 0);
        });
        setAudioTransportStatus(
          receivedAudio
            ? `控制端音频 RTP：${packetsReceived} 包 / ${bytesReceived.toLocaleString()} 字节，音量值 ${audioLevel.toFixed(3)}`
            : "控制端尚未收到远端音频 RTP 数据。",
        );
        if (Date.now() - lastStatsLogAt >= 15_000) {
          recordDiagnosticEvent("audio_inbound_rtp_stats", packetsReceived, bytesReceived);
          lastStatsLogAt = Date.now();
        }
      } catch (cause) {
        if (cancelled) return;
        const detail = cause instanceof Error ? cause.message : String(cause);
        setAudioTransportStatus(`无法读取接收音频统计：${detail}`);
        if (!statsReadErrorReported) {
          statsReadErrorReported = true;
          recordDiagnosticEvent("audio_inbound_stats_read_failed");
        }
      }
    };
    void sampleAudio();
    const timer = window.setInterval(() => void sampleAudio(), 2_000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [connectionStatus, playRemoteSystemAudio]);

  useEffect(() => {
    if (!standaloneDeviceId && deviceLayout === "list" && connectionDeviceId) disconnectFromDevice();
  }, [deviceLayout, standaloneDeviceId]);

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
      localStorage.setItem(SYSTEM_AUDIO_PLAY_STORAGE_KEY, String(playRemoteSystemAudio));
      localStorage.setItem(SYSTEM_AUDIO_SHARE_STORAGE_KEY, String(shareSystemAudio));
    } catch (cause) {
      const detail = cause instanceof Error ? cause.message : String(cause);
      setStorageError(`无法保存应用偏好：${detail}`);
    }
  }, [deviceLayout, clipboardEnabled, playRemoteSystemAudio, shareSystemAudio]);

  useEffect(() => {
    if (!isTauri()) return;
    void invoke("set_clipboard_sync_enabled", { enabled: clipboardEnabled })
      .catch((cause: unknown) => {
        const detail = cause instanceof Error ? cause.message : String(cause);
        setStorageError(`无法设置剪贴板同步：${detail}`);
      });
  }, [clipboardEnabled]);

  useEffect(() => {
    if (!isTauri()) return;
    void invoke("set_system_audio_sync_enabled", { enabled: shareSystemAudio })
      .catch((cause: unknown) => {
        const detail = cause instanceof Error ? cause.message : String(cause);
        setStorageError(`无法设置系统声音共享：${detail}`);
      });
  }, [shareSystemAudio]);

  useEffect(() => {
    if (!isTauri()) return;
    let cancelled = false;
    let unlisten: (() => void) | undefined;
    void listen<string>("agent-audio-status", ({ payload }) => setAudioStatus(payload))
      .then((stopListening) => {
        if (cancelled) stopListening();
        else unlisten = stopListening;
      })
      .catch((cause: unknown) => {
        const detail = cause instanceof Error ? cause.message : String(cause);
        setAudioStatus(`无法读取被控端声音状态：${detail}`);
      });
    return () => {
      cancelled = true;
      unlisten?.();
    };
  }, []);

  useEffect(() => {
    try {
      if (selectedDisplayId === null) {
        localStorage.removeItem(CAPTURE_DISPLAY_STORAGE_KEY);
      } else {
        localStorage.setItem(CAPTURE_DISPLAY_STORAGE_KEY, String(selectedDisplayId));
      }
    } catch (cause) {
      const detail = cause instanceof Error ? cause.message : String(cause);
      setDisplaySettingsError(`无法保存显示器偏好：${detail}`);
    }
    if (!isTauri()) return;
    void invoke("set_capture_display", { displayId: selectedDisplayId })
      .then(() => setDisplaySettingsError(""))
      .catch((cause: unknown) => {
        const detail = cause instanceof Error ? cause.message : String(cause);
        setDisplaySettingsError(`无法应用显示器选择：${detail}`);
      });
  }, [selectedDisplayId]);

  useEffect(() => {
    if (!isTauri()) return;
    let cancelled = false;
    let unlistenRequest: (() => void) | undefined;
    let unlistenRequestEnded: (() => void) | undefined;
    let unlistenStatus: (() => void) | undefined;
    let unlistenAgentConnection: (() => void) | undefined;
    let unlistenAgentPeerIdentity: (() => void) | undefined;
    let unlistenClipboard: (() => void) | undefined;
    let unlistenFileOffer: (() => void) | undefined;
    let unlistenFileReceived: (() => void) | undefined;
    let unlistenMainFocus: (() => void) | undefined;

    void getCurrentWindow().onFocusChanged(({ payload: focused }) => {
      if (focused) {
        void getCurrentWindow().requestUserAttention(null).catch((cause: unknown) => {
          const detail = cause instanceof Error ? cause.message : String(cause);
          setAgentStatus(`无法清除窗口提示：${detail}`);
        });
      }
    }).then((unlisten) => {
      if (cancelled) {
        unlisten();
      } else {
        unlistenMainFocus = unlisten;
      }
    }).catch((cause: unknown) => {
      const detail = cause instanceof Error ? cause.message : String(cause);
      setAgentStatus(`无法监听窗口焦点：${detail}`);
    });

    void Promise.all([
      listen<ConnectionRequest>("remote-connection-request", (event) => {
        if (!isMainWindow) return;
        setPendingRequest(event.payload);
      }),
      listen<string>("remote-connection-request-ended", (event) => {
        if (!isMainWindow) return;
        setPendingRequest((current) =>
          current?.requestId === event.payload ? null : current,
        );
      }),
      listen<string>("agent-status", (event) => {
        if (!isMainWindow) return;
        setAgentStatus(event.payload);
        if (event.payload === "远程服务已停止") {
          setAgentRunning(false);
          setAgentConnected(false);
        }
      }),
      listen<boolean>("agent-connection-state", (event) => {
        if (!isMainWindow) return;
        setAgentConnected(event.payload);
        if (!event.payload) {
          agentPeerIdentityRef.current = "";
          clipboardSessionAllowedRef.current = false;
          if (pendingAgentFileRef.current) {
            recordDiagnosticEvent("file_transfer_interrupted");
            setAgentClipboardStatus("连接已断开，续传进度已保留；重新连接后再次选择同一文件即可继续。");
          }
          pendingAgentFileRef.current = null;
          updateActiveFileTransfer(null);
          setPendingClipboardOffer(null);
        }
      }),
      listen<string>("agent-peer-identity", ({ payload }) => {
        if (isMainWindow) agentPeerIdentityRef.current = payload;
      }),
      listen<ClipboardOffer>("remote-clipboard-offer", (event) => {
        if (!isMainWindow) return;
        recordDiagnosticEvent(
          "clipboard_text_received",
          new TextEncoder().encode(event.payload.text).byteLength,
        );
        if (clipboardSessionAllowedRef.current) {
          void invoke("set_system_clipboard_text", { text: event.payload.text })
            .then(() => setTransferStatus("已将对端文本写入本机剪贴板（本次连接已授权）。"))
            .catch((cause: unknown) => {
              const detail = cause instanceof Error ? cause.message : String(cause);
              recordDiagnosticEvent("clipboard_sync_failed");
              setTransferStatus(`无法写入剪贴板：${detail}`);
            })
            .finally(restoreRemoteFocus);
        } else {
          setPendingClipboardOffer(event.payload);
          void focusMainWindow();
        }
      }),
      listen<Omit<FileOffer, "direction">>("remote-file-offer", (event) => {
        if (!isMainWindow) return;
        recordDiagnosticEvent("file_transfer_offer_received", event.payload.size);
        setPendingFileOffer({ ...event.payload, direction: "to-agent" });
        void focusMainWindow();
      }),
      listen<{
        transferId: string;
        fileName: string;
        savedPath: string;
        receivedBytes: number;
      }>("remote-file-received", (event) => {
        if (!isMainWindow) return;
        if (activeFileTransferRef.current?.transferId === event.payload.transferId) {
          updateActiveFileTransfer(null);
        }
        setTransferStatus(`已接收 ${event.payload.fileName}，保存至 ${event.payload.savedPath}`);
      }),
    ]).then(([requestListener, requestEndedListener, statusListener, agentConnectionListener, peerIdentityListener, clipboardListener, fileListener, receivedListener]) => {
      if (cancelled) {
        requestListener();
        requestEndedListener();
        statusListener();
        agentConnectionListener();
        peerIdentityListener();
        clipboardListener();
        fileListener();
        receivedListener();
        return;
      }
      unlistenRequest = requestListener;
      unlistenRequestEnded = requestEndedListener;
      unlistenStatus = statusListener;
      unlistenAgentConnection = agentConnectionListener;
      unlistenAgentPeerIdentity = peerIdentityListener;
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
      unlistenAgentConnection?.();
      unlistenAgentPeerIdentity?.();
      unlistenClipboard?.();
      unlistenFileOffer?.();
      unlistenFileReceived?.();
      unlistenMainFocus?.();
    };
  }, []);

  useEffect(() => {
    if (!isTauri() || !agentRunning) return;
    let cancelled = false;
    const pollPendingRequest = () => {
      void invoke<ConnectionRequest | null>("get_pending_connection_request")
        .then((request) => {
          if (!cancelled) {
            setPendingRequest((current) =>
              current?.requestId === request?.requestId ? current : request,
            );
          }
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
  }, [agentRunning, isMainWindow]);

  useEffect(() => {
    if (!isTauri()) return;
    let cancelled = false;
    let unlistenDecision: (() => void) | undefined;
    let unlistenStatus: (() => void) | undefined;
    let unlistenClipboard: (() => void) | undefined;
    let unlistenFileControl: (() => void) | undefined;
    let unlistenFileProgress: (() => void) | undefined;
    let unlistenFileComplete: (() => void) | undefined;
    let unlistenFileError: (() => void) | undefined;
    void Promise.all([
      listen<{
        transferId: string;
        accepted: boolean;
        resumeOffset: number;
        error?: string;
      }>(
        "agent-file-transfer-decision",
        ({ payload }) => {
          const pending = pendingAgentFileRef.current;
          if (!pending || pending.transferId !== payload.transferId) {
            if (payload.error) setAgentClipboardStatus(payload.error);
            return;
          }
          if (payload.error) {
            recordDiagnosticEvent("file_transfer_failed");
            pendingAgentFileRef.current = null;
            updateActiveFileTransfer(null);
            setAgentClipboardStatus(payload.error);
          } else if (payload.accepted) {
            void transmitAgentFile(pending.transferId, pending.file, payload.resumeOffset);
          } else {
            recordDiagnosticEvent("file_transfer_failed");
            pendingAgentFileRef.current = null;
            updateActiveFileTransfer(null);
            setAgentClipboardStatus(`${pending.file.name}：控制端拒绝接收。`);
          }
        },
      ),
      listen<string>("agent-file-transfer-status", ({ payload }) => {
        setAgentClipboardStatus(payload);
      }),
      listen<string>("clipboard-status", ({ payload }) => {
        setAgentClipboardStatus(payload);
        recordDiagnosticEvent("clipboard_sync_failed");
      }),
      listen<{ transferId: string; action: "pause" | "resume" | "cancel" }>(
        "agent-file-transfer-control",
        ({ payload }) => {
          const pending = pendingAgentFileRef.current;
          if (pending?.transferId === payload.transferId) {
            if (payload.action === "cancel") {
              pending.cancelled = true;
              clearFileTransferIdentity(pending.transferId);
              pendingAgentFileRef.current = null;
              updateActiveFileTransfer(null);
              setAgentClipboardStatus("对端已取消文件传输。");
              return;
            }
            pending.paused = payload.action === "pause";
          }
          const active = activeFileTransferRef.current;
          if (active?.transferId === payload.transferId) {
            if (payload.action === "cancel") {
              updateActiveFileTransfer(null);
              setAgentClipboardStatus(`已取消 ${active.fileName} 的文件传输。`);
            } else {
              updateActiveFileTransfer({ ...active, paused: payload.action === "pause" });
              setAgentClipboardStatus(payload.action === "pause" ? "文件传输已暂停。" : "文件传输已继续。");
            }
          }
        },
      ),
      listen<{ transferId: string; receivedBytes: number }>(
        "agent-file-transfer-progress",
        ({ payload }) => {
          const active = activeFileTransferRef.current;
          if (active?.transferId === payload.transferId) {
            updateActiveFileTransfer({ ...active, transferred: payload.receivedBytes });
          }
        },
      ),
      listen<{ transferId: string }>(
        "agent-file-transfer-complete",
        ({ payload }) => {
          if (pendingAgentFileRef.current?.transferId === payload.transferId) {
            pendingAgentFileRef.current = null;
            updateActiveFileTransfer(null);
          }
          clearFileTransferIdentity(payload.transferId);
          setAgentClipboardStatus("文件已成功送达控制端。");
        },
      ),
      listen<{ transferId: string; message: string }>(
        "agent-file-transfer-error",
        ({ payload }) => {
          if (pendingAgentFileRef.current?.transferId === payload.transferId) {
            pendingAgentFileRef.current = null;
            updateActiveFileTransfer(null);
          }
          setAgentClipboardStatus(`文件接收端报告失败：${payload.message}。续传标识已保留。`);
        },
      ),
    ]).then(([decisionListener, statusListener, clipboardListener, controlListener, progressListener, completeListener, errorListener]) => {
      if (cancelled) {
        decisionListener();
        statusListener();
        clipboardListener();
        controlListener();
        progressListener();
        completeListener();
        errorListener();
        return;
      }
      unlistenDecision = decisionListener;
      unlistenStatus = statusListener;
      unlistenClipboard = clipboardListener;
      unlistenFileControl = controlListener;
      unlistenFileProgress = progressListener;
      unlistenFileComplete = completeListener;
      unlistenFileError = errorListener;
    }).catch((cause: unknown) => {
      const detail = cause instanceof Error ? cause.message : String(cause);
      setAgentStatus(`无法接收剪贴板或文件传输状态：${detail}`);
    });
    return () => {
      cancelled = true;
      unlistenDecision?.();
      unlistenStatus?.();
      unlistenClipboard?.();
      unlistenFileControl?.();
      unlistenFileProgress?.();
      unlistenFileComplete?.();
      unlistenFileError?.();
    };
  }, []);

  useEffect(() => {
    if (pendingRequest && isMainWindow) void focusMainWindow();
  }, [isMainWindow, pendingRequest]);

  useEffect(() => () => {
    connectionRef.current?.close();
    controlChannelRef.current?.close();
    peerConnectionRef.current?.close();
  }, []);

  useEffect(() => {
    const updateFullscreenState = () => {
      const isFullscreen = document.fullscreenElement === viewerRef.current;
      setIsDomFullscreen(isFullscreen);
      if (!isFullscreen) {
        (navigator as Navigator & { keyboard?: KeyboardLock }).keyboard?.unlock();
        setKeyboardLockWarning("");
      }
    };
    document.addEventListener("fullscreenchange", updateFullscreenState);
    return () => {
      document.removeEventListener("fullscreenchange", updateFullscreenState);
      (navigator as Navigator & { keyboard?: KeyboardLock }).keyboard?.unlock();
    };
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

  useEffect(() => {
    const peer = peerConnectionRef.current;
    if (connectionStatus === "connected" && peer && remoteAudioStreamRef.current) {
      startRemoteAudioPlayback(peer);
    }
  }, [connectionStatus]);

  useEffect(() => {
    if (connectionStatus !== "connected") return;
    const releaseRemoteKeys = () => sendInput({ type: "release_all" });
    const onKey = (event: KeyboardEvent) => {
      if (
        event.isComposing ||
        isAdding ||
        showSettings ||
        pendingRequest ||
        pendingClipboardOffer ||
        pendingFileOffer ||
        pendingRemoval
      ) return;
      if (
        (event.altKey && (event.key === "Tab" || event.key === "F4")) ||
        (event.metaKey && ["Tab", "q", "w", "h", "m"].includes(event.key.toLowerCase())) ||
        (event.ctrlKey && event.key === "Tab")
      ) return;
      const target = event.target;
      if (
        target instanceof HTMLElement &&
        target.closest("input, textarea, select, button, a, [contenteditable='true'], [role='dialog']")
      ) return;
      event.preventDefault();
      event.stopPropagation();
      sendInput({
        type: "key",
        code: event.code,
        key: event.key,
        down: event.type === "keydown",
      });
    };
    window.addEventListener("keydown", onKey, true);
    window.addEventListener("keyup", onKey, true);
    window.addEventListener("blur", releaseRemoteKeys);
    return () => {
      window.removeEventListener("keydown", onKey, true);
      window.removeEventListener("keyup", onKey, true);
      window.removeEventListener("blur", releaseRemoteKeys);
      releaseRemoteKeys();
    };
  }, [
    connectionStatus,
    isAdding,
    pendingClipboardOffer,
    pendingFileOffer,
    pendingRemoval,
    pendingRequest,
    showSettings,
  ]);

  const activeDevice = useMemo(
    () => devices.find((device) => device.id === activeId) ?? null,
    [activeId, devices],
  );
  const clipboardSettingLocked =
    connectionStatus !== "idle" ||
    agentConnected ||
    Object.values(standaloneWindowStatuses).some(
      (status) => status === "opening" || status === "connecting" ||
        status === "waitingApproval" || status === "connected",
    );
  const playRemoteAudioSettingLocked =
    connectionStatus !== "idle" ||
    Object.values(standaloneWindowStatuses).some(
      (status) => status === "opening" || status === "connecting" ||
        status === "waitingApproval" || status === "connected",
    );
  const shareSystemAudioSettingLocked = agentConnected || pendingRequest !== null;

  async function saveDevice(event: React.FormEvent<HTMLFormElement>) {
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
    if (isTauri()) {
      setIsSavingDevice(true);
      try {
        if (await invoke<boolean>("is_local_device_address", { address: normalizedAddress })) {
          setError("不能将本机自己添加为远程设备。");
          return;
        }
      } catch (cause) {
        const detail = cause instanceof Error ? cause.message : String(cause);
        setError(`无法验证设备地址是否属于本机：${detail}`);
        return;
      } finally {
        setIsSavingDevice(false);
      }
    } else if (["localhost", "127.0.0.1", "::1"].includes(
      new URL(`http://${normalizedAddress}`).hostname.replace(/^\[|\]$/g, ""),
    )) {
      setError("不能将本机自己添加为远程设备。");
      return;
    }
    if (devices.some(
      (device) =>
        device.id !== editingDevice?.id &&
        isValidAddress(device.address) &&
        normalizeAddress(device.address) === normalizedAddress,
    )) {
      setError("这个地址已在设备列表中。");
      return;
    }

    if (editingDevice) {
      setDevices((current) => current.map((device) =>
        device.id === editingDevice.id
          ? { ...device, name: trimmedName, address: normalizedAddress }
          : device,
      ));
    } else {
      const device = {
        id: crypto.randomUUID(),
        name: trimmedName,
        address: normalizedAddress,
      };
      setDevices((current) => [...current, device]);
      setActiveId(device.id);
    }
    setName("");
    setAddress("");
    setError("");
    setIsAdding(false);
    setEditingDevice(null);
    setDiscoveredDevices([]);
    setDiscoveryStatus("");
  }

  function closeDeviceForm() {
    setIsAdding(false);
    setEditingDevice(null);
    setName("");
    setAddress("");
    setError("");
    setDiscoveredDevices([]);
    setDiscoveryStatus("");
  }

  async function discoverDevices() {
    if (!isTauri()) {
      setDiscoveryStatus("设备发现仅在 Lantern 桌面应用中可用。");
      return;
    }
    setIsDiscovering(true);
    setDiscoveryStatus("");
    recordDiagnosticEvent("discovery_started");
    try {
      const discovered = await invoke<DiscoveredDevice[]>("discover_lan_devices");
      setDiscoveredDevices(discovered);
      recordDiagnosticEvent("discovery_completed", discovered.length);
      setDiscoveryStatus(discovered.length ? `找到 ${discovered.length} 台正在运行被控端的设备。` : "未发现其他设备；请确认对端已启动被控端并处于同一局域网。");
    } catch (cause) {
      const detail = cause instanceof Error ? cause.message : String(cause);
      recordDiagnosticEvent("discovery_failed");
      setDiscoveryStatus(`扫描失败：${detail}`);
    } finally {
      setIsDiscovering(false);
    }
  }

  function removeDevice(id: string) {
    setDevices((current) => current.filter((device) => device.id !== id));
    if (connectionDeviceId === id) {
      disconnectFromDevice();
    }
    if (standaloneWindowStatuses[id]) void closeRemoteWindow(id);
    setActiveId((current) => (current === id ? null : current));
  }

  async function toggleRemoteFullscreen() {
    const viewer = viewerRef.current;
    if (!viewer) return;
    try {
      if (isTauri()) {
        await getCurrentWindow().setFullscreen(!isNativeFullscreen);
        setIsNativeFullscreen((current) => !current);
        setKeyboardLockWarning("");
        return;
      }
      if (document.fullscreenElement === viewer) {
        await document.exitFullscreen();
      } else {
        const keyboard = (navigator as Navigator & { keyboard?: KeyboardLock }).keyboard;
        if (keyboard) {
          try {
            await keyboard.lock(["Escape"]);
            setKeyboardLockWarning("");
          } catch (cause) {
            const detail = cause instanceof Error ? cause.message : String(cause);
            setKeyboardLockWarning(`此系统未能锁定 Esc 键，全屏时按 Esc 仍可能退出：${detail}`);
          }
        } else {
          setKeyboardLockWarning("此系统不支持锁定 Esc 键，全屏时按 Esc 仍可能退出。");
        }
        await viewer.requestFullscreen();
      }
    } catch (cause) {
      (navigator as Navigator & { keyboard?: KeyboardLock }).keyboard?.unlock();
      setKeyboardLockWarning("");
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
    void refreshDiagnosticLog();
    setTrustedDevicesError("");
    const [trustedDevicesResult, displaysResult] = await Promise.allSettled([
      invoke<TrustedDevice[]>("get_trusted_devices"),
      invoke<AvailableDisplay[]>("get_available_displays"),
    ]);
    if (trustedDevicesResult.status === "fulfilled") {
      setTrustedDevices(trustedDevicesResult.value);
    } else {
      const cause = trustedDevicesResult.reason;
      const detail = cause instanceof Error ? cause.message : String(cause);
      setTrustedDevicesError(`无法读取信任设备：${detail}`);
    }
    if (displaysResult.status === "fulfilled") {
      setAvailableDisplays(displaysResult.value);
      if (selectedDisplayId !== null &&
          !displaysResult.value.some((display) => display.id === selectedDisplayId)) {
        setSelectedDisplayId(null);
      }
    } else {
      const cause = displaysResult.reason;
      const detail = cause instanceof Error ? cause.message : String(cause);
      setDisplaySettingsError(`无法读取显示器列表：${detail}`);
    }
  }

  async function refreshDiagnosticLog() {
    if (!isTauri()) {
      setDiagnosticLogError("诊断日志仅在 Lantern 桌面应用中可用。");
      return;
    }
    setDiagnosticLogError("");
    try {
      setDiagnosticLog(await invoke<string>("get_diagnostic_log"));
    } catch (cause) {
      const detail = cause instanceof Error ? cause.message : String(cause);
      setDiagnosticLogError(`无法读取诊断日志：${detail}`);
    }
  }

  function recordDiagnosticEvent(
    event: DiagnosticEvent,
    value?: number,
    value2?: number,
  ) {
    if (!isTauri()) return;
    const args: { event: DiagnosticEvent; value?: number; value2?: number } = { event };
    if (value !== undefined) args.value = value;
    if (value2 !== undefined) args.value2 = value2;
    void invoke("log_diagnostic_event", args).catch((cause: unknown) => {
      const detail = cause instanceof Error ? cause.message : String(cause);
      setDiagnosticLogError(`无法写入本机诊断日志：${detail}`);
    });
  }

  async function copyDiagnosticLog() {
    try {
      await navigator.clipboard.writeText(diagnosticLog);
      setDiagnosticLogError("诊断日志已复制到剪贴板。");
    } catch (cause) {
      const detail = cause instanceof Error ? cause.message : String(cause);
      setDiagnosticLogError(`无法复制诊断日志：${detail}`);
    }
  }

  async function updateCloseToTray(enabled: boolean) {
    if (!isTauri() || savingCloseToTray) return;
    setSavingCloseToTray(true);
    setCloseToTrayError("");
    try {
      await invoke("set_close_to_tray", { enabled });
      setCloseToTray(enabled);
    } catch (cause) {
      const detail = cause instanceof Error ? cause.message : String(cause);
      setCloseToTrayError(`无法保存关闭窗口设置：${detail}`);
    } finally {
      setSavingCloseToTray(false);
    }
  }

  async function openRemoteWindow(device: Device) {
    if (!isTauri()) {
      setTransferStatus("独立控制窗口仅可在 Lantern 桌面应用中打开。");
      return;
    }
    try {
      const label = `remote-${device.id}`;
      const existing = await WebviewWindow.getByLabel(label);
      if (existing) {
        setStandaloneWindowStatuses((current) => ({
          ...current,
          [device.id]: current[device.id] ?? "opening",
        }));
        await existing.show();
        await existing.setFocus();
        await emit("remote-window-status-request", { deviceId: device.id });
        return;
      }
      if (connectionDeviceId === device.id) disconnectFromDevice();
      setStandaloneWindowStatuses((current) => ({ ...current, [device.id]: "opening" }));
      const remoteWindow = new WebviewWindow(label, {
        url: `index.html?remoteDevice=${encodeURIComponent(device.id)}`,
        title: `Lantern · ${device.name}`,
        width: 1280,
        height: 820,
        minWidth: 760,
        minHeight: 480,
        resizable: true,
      });
      remoteWindow.once("tauri://destroyed", () => {
        setStandaloneWindowStatuses((current) => {
          const { [device.id]: _closed, ...remaining } = current;
          return remaining;
        });
      });
      remoteWindow.once("tauri://error", (event) => {
        setStandaloneWindowStatuses((current) => {
          const { [device.id]: _failed, ...remaining } = current;
          return remaining;
        });
        setTransferStatus(`无法打开独立控制窗口：${String(event.payload)}`);
      });
    } catch (cause) {
      setStandaloneWindowStatuses((current) => {
        const { [device.id]: _failed, ...remaining } = current;
        return remaining;
      });
      const detail = cause instanceof Error ? cause.message : String(cause);
      setTransferStatus(`无法打开独立控制窗口：${detail}`);
    }
  }

  async function closeRemoteWindow(deviceId: string) {
    try {
      const remoteWindow = await WebviewWindow.getByLabel(`remote-${deviceId}`);
      if (remoteWindow) await remoteWindow.close();
    } catch (cause) {
      const detail = cause instanceof Error ? cause.message : String(cause);
      setTransferStatus(`无法关闭独立控制窗口：${detail}`);
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
      recordDiagnosticEvent("agent_started");
      setAgentAddress(address);
      setAgentRunning(true);
      setAgentStatus("正在局域网等待连接。新连接必须在被控端本机批准。");
    } catch (cause) {
      const detail = cause instanceof Error ? cause.message : String(cause);
      recordDiagnosticEvent("agent_start_failed");
      setAgentStatus(`启动失败：${detail}`);
    }
  }

  async function stopAgent() {
    try {
      await invoke("stop_agent");
      recordDiagnosticEvent("agent_stopped");
      setAgentRunning(false);
      setAgentConnected(false);
      setPendingRequest(null);
      setAgentStatus("远程服务已停止");
    } catch (cause) {
      const detail = cause instanceof Error ? cause.message : String(cause);
      recordDiagnosticEvent("agent_stop_failed");
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
    disconnectFromDevice(false);
    recordDiagnosticEvent("connection_started");
    connectionTimeoutRef.current = window.setTimeout(() => {
      if (connectionRef.current &&
          !(peerConnectionRef.current?.connectionState === "connected" &&
            controlChannelRef.current?.readyState === "open")) {
        recordDiagnosticEvent("connection_failed");
        setConnectionStatus("error");
        setConnectionError("连接建立超时。请检查被控端审批状态以及局域网 UDP 防火墙设置，然后重试。");
        connectionRef.current.close();
      }
    }, 90_000);
    setActiveId(device.id);
    setConnectionDeviceId(device.id);
    setConnectionStatus("connecting");
    setConnectionError("");
    setEncoderStatus("");
    setAudioStatus(playRemoteSystemAudio ? "正在等待远端系统声音…" : "");
    setAudioTransportStatus("");
    setAudioPlaybackBlocked(false);

    try {
      const endpoint = new URL(`http://${device.address}`);
      if (!endpoint.port) endpoint.port = "4200";
      endpoint.protocol = "ws:";
      endpoint.pathname = "/ws";
      endpoint.search = "";
      endpoint.hash = "";

      const socket = new WebSocket(endpoint);
      socket.binaryType = "arraybuffer";
      connectionRef.current = socket;
      socket.onopen = () => {
        setConnectionStatus("waitingApproval");
        socket.send(JSON.stringify({
          type: "hello",
          device_name: "Lantern 控制端",
          quality: videoQuality,
          clipboard_enabled: clipboardEnabled,
          system_audio_enabled: playRemoteSystemAudio,
        }));
      };
      socket.onmessage = (event: MessageEvent<string | ArrayBuffer>) => {
        if (event.data instanceof ArrayBuffer) {
          const incoming = incomingFileTransferRef.current;
          if (!incoming) {
            setTransferStatus("收到未请求的文件数据，已忽略。");
            return;
          }
          incoming.receivedBytes += event.data.byteLength;
          if (incoming.receivedBytes > incoming.size) {
            incomingFileTransferRef.current = null;
            cancelControllerFileReceive(incoming.transferId, incoming.writeQueue);
            setTransferStatus("收到的文件数据超过声明大小，传输已中止。");
            socket.close();
            return;
          }
          const active = activeFileTransferRef.current;
          if (active?.transferId === incoming.transferId) {
            updateActiveFileTransfer({ ...active, transferred: incoming.receivedBytes });
          }
          const chunk = Array.from(new Uint8Array(event.data));
          incoming.writeQueue = incoming.writeQueue.then(async () => {
            if (incoming.writeError) return;
            await invoke("append_controller_file_chunk", {
              transferId: incoming.transferId,
              bytes: chunk,
            });
          }).catch((cause: unknown) => {
            incoming.writeError = cause instanceof Error ? cause.message : String(cause);
          });
          return;
        }
        let message: {
          type?: string;
          message?: string;
          status?: string;
          sdp?: string;
          candidate?: string;
          sdpMid?: string | null;
          sdpMLineIndex?: number | null;
          transfer_id?: string;
          action?: string;
          accepted?: boolean;
          text?: string;
          file_name?: string;
          size?: number;
          sha256?: string;
          resume_offset?: number;
        };
        try {
          message = JSON.parse(event.data) as typeof message;
        } catch {
          recordDiagnosticEvent("connection_failed");
          setConnectionStatus("error");
          setConnectionError("无法解析远程端的信令响应。");
          socket.close();
          return;
        }

        if (message.type === "approved") {
          recordDiagnosticEvent("connection_approved");
          setConnectionStatus("connecting");
          void startWebRtcConnection(socket, playRemoteSystemAudio);
        } else if (message.type === "audio_status" && typeof message.status === "string") {
          setAudioStatus(message.status);
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
              recordDiagnosticEvent("connection_failed");
              setConnectionStatus("error");
              setConnectionError(`无法协商 WebRTC 媒体连接：${detail}`);
              socket.close();
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
              recordDiagnosticEvent("connection_failed");
              setConnectionStatus("error");
              setConnectionError(`无法添加 WebRTC 网络候选：${detail}`);
              socket.close();
            });
          } else {
            pendingRemoteCandidatesRef.current.push(candidate);
          }
        } else if (message.type === "encoder_status" && message.status) {
          setEncoderStatus(message.status);
        } else if (message.type === "clipboard_offer" && typeof message.text === "string") {
          void receiveClipboardOffer(message.text);
        } else if (
          message.type === "file_offer" &&
          typeof message.transfer_id === "string" &&
          typeof message.file_name === "string" &&
          typeof message.size === "number" &&
          typeof message.sha256 === "string" &&
          /^[a-f0-9]{64}$/.test(message.sha256) &&
          message.size > 0 &&
          message.size <= 100 * 1024 * 1024
        ) {
          setPendingFileOffer({
            transferId: message.transfer_id,
            fileName: message.file_name,
            size: message.size,
            sha256: message.sha256,
            receivedBytes: 0,
            direction: "from-agent",
          });
          void focusCurrentWindow();
        } else if (message.type === "file_offer") {
          if (typeof message.transfer_id === "string") {
            socket.send(JSON.stringify({
              type: "file_transfer_decision",
              transfer_id: message.transfer_id,
              accepted: false,
            }));
          }
          setTransferStatus("收到无效的文件传输请求，已拒绝。");
        } else if (message.type === "file_complete" && message.transfer_id) {
          void finishIncomingAgentFile(socket, message.transfer_id);
        } else if (message.type === "file_transfer_decision" && message.transfer_id) {
          const session = pendingFileSendsRef.current.get(message.transfer_id);
          if (!session) return;
          if (!message.accepted) {
            pendingFileSendsRef.current.delete(message.transfer_id);
            updateActiveFileTransfer(null);
            setTransferStatus(`${session.file.name}：对端拒绝接收。`);
          } else {
            const resumeOffset = message.resume_offset ?? 0;
            if (!Number.isSafeInteger(resumeOffset) || resumeOffset < 0 || resumeOffset > session.file.size) {
              setTransferStatus("对端返回了无效的续传进度。");
              socket.close();
              return;
            }
            void transmitFile(socket, message.transfer_id, session.file, resumeOffset);
          }
        } else if (message.type === "file_transfer_complete" && message.transfer_id) {
          const session = pendingFileSendsRef.current.get(message.transfer_id);
          if (session) {
            clearFileTransferIdentity(message.transfer_id);
            pendingFileSendsRef.current.delete(message.transfer_id);
            recordDiagnosticEvent("file_transfer_completed", session.file.size);
          }
          setTransferStatus("文件已成功发送。");
          updateActiveFileTransfer(null);
        } else if (
          message.type === "file_transfer_control" &&
          typeof message.transfer_id === "string" &&
          ["pause", "resume", "cancel"].includes(message.action ?? "")
        ) {
          const active = activeFileTransferRef.current;
          if (active?.transferId === message.transfer_id) {
            const controlEvent = message.action === "pause"
              ? "file_transfer_paused"
              : message.action === "resume"
                ? "file_transfer_resumed"
                : "file_transfer_cancelled";
            recordDiagnosticEvent(controlEvent, active.transferred, active.size);
            if (message.action === "cancel") {
              if (active.mode === "send") {
                const session = pendingFileSendsRef.current.get(message.transfer_id);
                if (session) {
                  session.cancelled = true;
                  clearFileTransferIdentity(message.transfer_id);
                }
                pendingFileSendsRef.current.delete(message.transfer_id);
              } else {
                const incoming = incomingFileTransferRef.current;
                if (incoming?.transferId === message.transfer_id) {
                  incomingFileTransferRef.current = null;
                  cancelControllerFileReceive(message.transfer_id, incoming.writeQueue);
                }
              }
              updateActiveFileTransfer(null);
              setTransferStatus(`对端已取消 ${active.fileName} 的文件传输。`);
            } else {
              const session = pendingFileSendsRef.current.get(message.transfer_id);
              if (session) session.paused = message.action === "pause";
              updateActiveFileTransfer({ ...active, paused: message.action === "pause" });
              setTransferStatus(message.action === "pause" ? `对端已暂停 ${active.fileName}。` : `对端已继续 ${active.fileName}。`);
            }
          }
        } else if (message.type === "file_transfer_error" && message.message) {
          const transferId = typeof message.transfer_id === "string" ? message.transfer_id : "";
          const sending = transferId ? pendingFileSendsRef.current.get(transferId) : undefined;
          if (sending) pendingFileSendsRef.current.delete(transferId);
          const incoming = incomingFileTransferRef.current;
          if (incoming?.transferId === transferId) {
            incomingFileTransferRef.current = null;
            suspendControllerFileReceive(transferId, incoming.writeQueue);
          }
          if (activeFileTransferRef.current?.transferId === transferId) {
            updateActiveFileTransfer(null);
          }
          recordDiagnosticEvent("file_transfer_failed");
          setTransferStatus(
            sending
              ? `${sending.file.name}：${message.message}续传标识已保留，可重新连接后再次选择同一文件。`
              : message.message,
          );
        } else if (message.type === "clipboard_error" && message.message) {
          setTransferStatus(message.message);
        } else if (message.type === "error") {
          recordDiagnosticEvent("connection_failed");
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
        recordDiagnosticEvent("connection_failed");
        setConnectionStatus("error");
        setConnectionError("连接失败。请确认对端已启动、地址正确，且局域网未阻止 TCP 4200。");
        socket.close();
      };
      socket.onclose = () => {
        if (connectionRef.current !== socket) return;
        if (connectionTimeoutRef.current !== null) {
          window.clearTimeout(connectionTimeoutRef.current);
          connectionTimeoutRef.current = null;
        }
        if (connectionRecoveryTimerRef.current !== null) {
          window.clearTimeout(connectionRecoveryTimerRef.current);
          connectionRecoveryTimerRef.current = null;
        }
        recordDiagnosticEvent("connection_closed");
        connectionRef.current = null;
        const incomingFile = incomingFileTransferRef.current;
        incomingFileTransferRef.current = null;
        if (incomingFile) {
          recordDiagnosticEvent("file_transfer_interrupted");
          suspendControllerFileReceive(incomingFile.transferId, incomingFile.writeQueue);
        }
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
        remoteAudioStreamRef.current = null;
        if (remoteAudioRef.current) remoteAudioRef.current.srcObject = null;
        setAudioPlaybackBlocked(false);
        pendingRemoteCandidatesRef.current = [];
        if (pendingFileSendsRef.current.size > 0) {
          recordDiagnosticEvent("file_transfer_interrupted");
          setTransferStatus("连接已断开，续传进度已保留；重新连接后再次选择相同文件即可继续。");
          pendingFileSendsRef.current.clear();
        }
        updateActiveFileTransfer(null);
        if (remoteVideoRef.current) remoteVideoRef.current.srcObject = null;
        setScreenSize({ width: 0, height: 0 });
        setConnectionStatus((status) => status === "error" ? status : "idle");
        setConnectionDeviceId((id) => id === device.id ? null : id);
      };
    } catch (cause) {
      if (connectionTimeoutRef.current !== null) {
        window.clearTimeout(connectionTimeoutRef.current);
        connectionTimeoutRef.current = null;
      }
      if (connectionRecoveryTimerRef.current !== null) {
        window.clearTimeout(connectionRecoveryTimerRef.current);
        connectionRecoveryTimerRef.current = null;
      }
      const detail = cause instanceof Error ? cause.message : String(cause);
      recordDiagnosticEvent("connection_failed");
      setConnectionStatus("error");
      setConnectionError(`无法创建局域网连接：${detail}`);
    }
  }

  async function startWebRtcConnection(socket: WebSocket, receiveSystemAudio: boolean) {
    const peer = new RTCPeerConnection({ iceServers: [] });
    peerConnectionRef.current = peer;
    peer.addTransceiver("video", { direction: "recvonly" });
    if (receiveSystemAudio) {
      peer.addTransceiver("audio", { direction: "recvonly" });
    }

    const controlChannel = peer.createDataChannel("control");
    controlChannelRef.current = controlChannel;
    const markControlConnected = () => {
      if (
        peerConnectionRef.current === peer &&
        peer.connectionState === "connected" &&
        controlChannel.readyState === "open"
      ) {
        if (connectionTimeoutRef.current !== null) {
          window.clearTimeout(connectionTimeoutRef.current);
          connectionTimeoutRef.current = null;
        }
        if (connectionRecoveryTimerRef.current !== null) {
          window.clearTimeout(connectionRecoveryTimerRef.current);
          connectionRecoveryTimerRef.current = null;
        }
        setConnectionStatus("connected");
        recordDiagnosticEvent("connection_established");
        window.setTimeout(() => viewerRef.current?.focus({ preventScroll: true }), 0);
      }
    };
    controlChannel.onopen = () => {
      markControlConnected();
    };
    controlChannel.onerror = () => {
      if (peerConnectionRef.current !== peer) return;
      recordDiagnosticEvent("connection_failed");
      setConnectionStatus("error");
      setConnectionError("WebRTC 键鼠控制通道发生错误。");
      socket.close();
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
      if (event.track.kind === "audio") {
        recordDiagnosticEvent("audio_remote_track_received");
        remoteAudioStreamRef.current = event.streams[0] ?? new MediaStream([event.track]);
        event.track.onunmute = () => {
          if (peerConnectionRef.current !== peer) return;
          recordDiagnosticEvent("audio_remote_track_unmuted");
          setAudioStatus("已收到远端音频数据，正在启动播放。");
          startRemoteAudioPlayback(peer);
        };
        event.track.onmute = () => {
          if (peerConnectionRef.current === peer) {
            setAudioStatus("远端音频轨道暂时没有数据。");
          }
        };
        setAudioStatus("已连接远端音频轨道，等待声音数据。");
        startRemoteAudioPlayback(peer);
        return;
      }
      const stream = event.streams[0] ?? new MediaStream([event.track]);
      remoteStreamRef.current = stream;
      const video = remoteVideoRef.current;
      if (video) {
        video.srcObject = stream;
        void video.play().catch((cause: unknown) => {
          if (peerConnectionRef.current !== peer) return;
          const detail = cause instanceof Error ? cause.message : String(cause);
          recordDiagnosticEvent("connection_failed");
          setConnectionError(`无法播放远程视频流：${detail}`);
        });
      }
    };
    peer.onconnectionstatechange = () => {
      if (peerConnectionRef.current !== peer) return;
      if (peer.connectionState === "connected") {
        if (connectionRecoveryTimerRef.current !== null) {
          window.clearTimeout(connectionRecoveryTimerRef.current);
          connectionRecoveryTimerRef.current = null;
        }
        markControlConnected();
      } else if (peer.connectionState === "failed") {
        if (connectionTimeoutRef.current !== null) {
          window.clearTimeout(connectionTimeoutRef.current);
          connectionTimeoutRef.current = null;
        }
        recordDiagnosticEvent("connection_failed");
        setConnectionStatus("error");
        setConnectionError("WebRTC 媒体连接失败。请检查两台设备间的 UDP 防火墙设置。");
        socket.close();
      } else if (peer.connectionState === "disconnected") {
        if (connectionRecoveryTimerRef.current === null) {
          recordDiagnosticEvent("connection_interrupted");
          setTransferStatus("连接短暂中断，正在等待网络恢复…");
          connectionRecoveryTimerRef.current = window.setTimeout(() => {
            connectionRecoveryTimerRef.current = null;
            if (peerConnectionRef.current !== peer || peer.connectionState !== "disconnected") return;
            setConnectionStatus("error");
            setConnectionError("WebRTC 连接中断，已清理连接。请检查网络后手动重试。");
            socket.close();
          }, 5_000);
        }
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
      recordDiagnosticEvent("connection_failed");
      setConnectionStatus("error");
      setConnectionError(`无法启动 WebRTC 连接：${detail}`);
      socket.close();
    }
  }

  function startRemoteAudioPlayback(peer: RTCPeerConnection) {
    const audio = remoteAudioRef.current;
    const stream = remoteAudioStreamRef.current;
    if (!audio || !stream || peerConnectionRef.current !== peer) return;
    audio.srcObject = stream;
    audio.muted = false;
    audio.volume = 1;
    void audio.play()
      .then(() => {
        if (peerConnectionRef.current !== peer) return;
        recordDiagnosticEvent("audio_playback_started");
        setAudioPlaybackBlocked(false);
        setAudioStatus("正在播放远端系统声音。");
      })
      .catch(() => {
        if (peerConnectionRef.current !== peer) return;
        recordDiagnosticEvent("audio_playback_blocked");
        setAudioPlaybackBlocked(true);
        setAudioStatus("远端系统声音已就绪，需要手动开始播放。");
      });
  }

  function playRemoteAudio() {
    const audio = remoteAudioRef.current;
    if (!audio) return;
    void audio.play()
      .then(() => {
        recordDiagnosticEvent("audio_playback_started");
        setAudioPlaybackBlocked(false);
        setAudioStatus("正在播放远端系统声音。");
      })
      .catch((cause: unknown) => {
        const detail = cause instanceof Error ? cause.message : String(cause);
        recordDiagnosticEvent("audio_playback_failed");
        setAudioStatus(`无法播放远端系统声音：${detail}`);
      });
  }

  function disconnectFromDevice(closeStandaloneWindow = true) {
    if (connectionTimeoutRef.current !== null) {
      window.clearTimeout(connectionTimeoutRef.current);
      connectionTimeoutRef.current = null;
    }
    if (connectionRecoveryTimerRef.current !== null) {
      window.clearTimeout(connectionRecoveryTimerRef.current);
      connectionRecoveryTimerRef.current = null;
    }
    if (connectionRef.current || peerConnectionRef.current) {
      recordDiagnosticEvent("connection_closed");
    }
    if (activeFileTransferRef.current) {
      recordDiagnosticEvent("file_transfer_failed");
      setTransferStatus(`已断开连接，${activeFileTransferRef.current.fileName} 的续传进度已保留。`);
    }
    const incomingFile = incomingFileTransferRef.current;
    incomingFileTransferRef.current = null;
    if (incomingFile) {
      suspendControllerFileReceive(incomingFile.transferId, incomingFile.writeQueue);
    }
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
    remoteAudioStreamRef.current = null;
    if (remoteAudioRef.current) remoteAudioRef.current.srcObject = null;
    pendingRemoteCandidatesRef.current = [];
    pendingFileSendsRef.current.clear();
    updateActiveFileTransfer(null);
    clipboardSessionAllowedRef.current = false;
    setPendingClipboardOffer(null);
    if (remoteVideoRef.current) remoteVideoRef.current.srcObject = null;
    setAudioPlaybackBlocked(false);
    setAudioStatus("");
    setAudioTransportStatus("");
    setConnectionDeviceId(null);
    setConnectionStatus("idle");
    setConnectionError("");
    setScreenSize({ width: 0, height: 0 });
    setEncoderStatus("");
    if (standaloneDeviceId && isTauri() && closeStandaloneWindow) {
      standaloneClosingRef.current = true;
      void emit("remote-window-state", { deviceId: standaloneDeviceId, status: "closed" })
        .catch((cause: unknown) => {
          const detail = cause instanceof Error ? cause.message : String(cause);
          setConnectionError(`无法同步独立控制窗口状态：${detail}`);
        });
      void getCurrentWindow().close().catch((cause: unknown) => {
        const detail = cause instanceof Error ? cause.message : String(cause);
        setConnectionError(`无法关闭独立控制窗口：${detail}`);
      });
    }
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
      const text = await invoke<string>("get_system_clipboard_text");
      if (new TextEncoder().encode(text).byteLength > 48 * 1024) {
        setTransferStatus("剪贴板内容超过 48 KiB，未发送。");
        return;
      }
      socket.send(JSON.stringify({ type: "clipboard_text", text }));
      lastClipboardTextRef.current = text;
      setTransferStatus("已发送剪贴板内容，等待对端确认。");
    } catch (cause) {
      const detail = cause instanceof Error ? cause.message : String(cause);
      recordDiagnosticEvent("clipboard_sync_failed");
      setTransferStatus(`无法读取剪贴板：${detail}`);
    } finally {
      restoreRemoteFocus();
    }
  }

  async function sendAgentClipboard() {
    if (!agentConnected) {
      setAgentClipboardStatus("当前没有已连接的控制端。");
      return;
    }
    try {
      const text = await invoke<string>("get_system_clipboard_text");
      if (new TextEncoder().encode(text).byteLength > 48 * 1024) {
        setAgentClipboardStatus("剪贴板内容超过 48 KiB，未发送。");
        return;
      }
      await invoke("send_agent_clipboard", { text });
      setAgentClipboardStatus("已发送剪贴板内容，等待控制端确认。");
    } catch (cause) {
      const detail = cause instanceof Error ? cause.message : String(cause);
      recordDiagnosticEvent("clipboard_sync_failed");
      setAgentClipboardStatus(`无法发送剪贴板：${detail}`);
    }
  }

  async function receiveClipboardOffer(text: string) {
    const offer = { text };
    if (clipboardSessionAllowedRef.current) {
      try {
        await invoke("set_system_clipboard_text", { text });
        lastClipboardTextRef.current = text;
        setTransferStatus("已将对端文本写入本机剪贴板（本次连接已授权）。");
      } catch (cause) {
        const detail = cause instanceof Error ? cause.message : String(cause);
        recordDiagnosticEvent("clipboard_sync_failed");
        setTransferStatus(`无法写入剪贴板：${detail}`);
      } finally {
        restoreRemoteFocus();
      }
    } else {
      setPendingClipboardOffer(offer);
      void focusCurrentWindow();
    }
  }

  async function answerClipboardOffer(accepted: boolean) {
    const offer = pendingClipboardOffer;
    setPendingClipboardOffer(null);
    try {
      if (accepted && offer) {
        await invoke("set_system_clipboard_text", { text: offer.text });
        lastClipboardTextRef.current = offer.text;
        clipboardSessionAllowedRef.current = true;
        setTransferStatus("已将文本写入本机剪贴板；本次连接后续剪贴板接收将自动允许。");
      }
    } catch (cause) {
      const detail = cause instanceof Error ? cause.message : String(cause);
      recordDiagnosticEvent("clipboard_sync_failed");
      setTransferStatus(`无法写入剪贴板：${detail}`);
    } finally {
      restoreRemoteFocus();
    }
  }

  async function focusMainWindow() {
    if (!isTauri()) return;
    try {
      const mainWindow = await WebviewWindow.getByLabel("main");
      if (!mainWindow) throw new Error("主窗口不存在。");
      await mainWindow.show();
      await mainWindow.setFocus();
      if (!(await mainWindow.isFocused())) {
        await mainWindow.requestUserAttention(UserAttentionType.Critical);
        setAgentStatus("有待处理的确认，请切换到 Lantern 主窗口处理。");
      }
    } catch (cause) {
      const detail = cause instanceof Error ? cause.message : String(cause);
      setAgentStatus(`有待处理的请求，但无法显示主窗口：${detail}`);
    }
  }

  async function focusCurrentWindow() {
    if (!isTauri()) return;
    try {
      const currentWindow = getCurrentWindow();
      await currentWindow.show();
      await currentWindow.setFocus();
    } catch (cause) {
      const detail = cause instanceof Error ? cause.message : String(cause);
      setTransferStatus(`有待处理的剪贴板确认，但无法聚焦窗口：${detail}`);
    }
  }

  async function answerFileOffer(accepted: boolean) {
    const offer = pendingFileOffer;
    setPendingFileOffer(null);
    if (!offer) return;
    try {
      recordDiagnosticEvent(
        accepted ? "file_transfer_accepted" : "file_transfer_rejected",
        offer.size,
      );
      if (offer.direction === "from-agent") {
        const socket = connectionRef.current;
        if (!socket || socket.readyState !== WebSocket.OPEN) throw new Error("控制连接已关闭。");
        if (accepted) {
          try {
            const resumeOffset = await invoke<number>("begin_controller_file_receive", {
              transferId: offer.transferId,
              fileName: offer.fileName,
              size: offer.size,
              sha256: offer.sha256,
            });
            incomingFileTransferRef.current = {
              transferId: offer.transferId,
              fileName: offer.fileName,
              size: offer.size,
              receivedBytes: resumeOffset,
              writeQueue: Promise.resolve(),
              writeError: null,
            };
            updateActiveFileTransfer({
              transferId: offer.transferId,
              fileName: offer.fileName,
              size: offer.size,
              transferred: resumeOffset,
              paused: false,
              path: "websocket",
              mode: "receive",
            });
          } catch (cause) {
            socket.send(JSON.stringify({
              type: "file_transfer_decision",
              transfer_id: offer.transferId,
              accepted: false,
            }));
            throw cause;
          }
        }
        socket.send(JSON.stringify({
          type: "file_transfer_decision",
          transfer_id: offer.transferId,
          accepted,
          resume_offset: accepted
            ? incomingFileTransferRef.current?.receivedBytes ?? 0
            : 0,
        }));
      } else {
        await invoke("answer_file_transfer", {
          transferId: offer.transferId,
          accepted,
        });
        if (accepted) {
          updateActiveFileTransfer({
            transferId: offer.transferId,
            fileName: offer.fileName,
            size: offer.size,
            transferred: offer.receivedBytes,
            paused: false,
            path: "tauri",
            mode: "receive",
          });
        }
      }
      setTransferStatus(accepted ? `正在接收 ${offer.fileName}…` : `已拒绝接收 ${offer.fileName}。`);
    } catch (cause) {
      const detail = cause instanceof Error ? cause.message : String(cause);
      recordDiagnosticEvent("file_transfer_failed");
      setTransferStatus(`无法处理文件请求：${detail}`);
    } finally {
      restoreRemoteFocus();
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
    try {
      setTransferStatus(`正在校验 ${file.name}，请稍候…`);
      const sha256 = await hashFile(file);
      const targetIdentity = connectionDeviceId ?? activeId ?? "remote-device";
      const transferId = await getOrCreateFileTransferId(file.name, sha256, targetIdentity);
      if (connectionRef.current !== socket || socket.readyState !== WebSocket.OPEN) {
        throw new Error("校验完成时连接已关闭；续传标识已保留，请重新连接后再次选择文件。");
      }
      pendingFileSendsRef.current.set(transferId, {
        file,
        sha256,
        paused: false,
        cancelled: false,
      });
      updateActiveFileTransfer({
        transferId,
        fileName: file.name,
        size: file.size,
        transferred: 0,
        paused: false,
        path: "websocket",
        mode: "send",
      });
      socket.send(JSON.stringify({
        type: "file_offer",
        transfer_id: transferId,
        file_name: file.name,
        size: file.size,
        sha256,
      }));
      recordDiagnosticEvent("file_transfer_started", file.size);
      setTransferStatus(`已请求发送 ${file.name}，等待对端批准。`);
    } catch (cause) {
      const detail = cause instanceof Error ? cause.message : String(cause);
      setTransferStatus(`无法准备文件续传：${detail}`);
    }
  }

  async function transmitFile(socket: WebSocket, transferId: string, file: File, resumeOffset: number) {
    let offset = resumeOffset;
    let loggedProgressBucket = Math.floor(resumeOffset / (5 * 1024 * 1024));
    try {
      const chunkSize = 48 * 1024;
      while (offset < file.size) {
        const session = pendingFileSendsRef.current.get(transferId);
        if (!session) throw new Error("文件传输已取消。");
        while (session.paused && !session.cancelled) {
          await new Promise((resolve) => window.setTimeout(resolve, 100));
        }
        if (session.cancelled) throw new Error("文件传输已取消。");
        while (socket.bufferedAmount > 512 * 1024) {
          if (session.cancelled) throw new Error("文件传输已取消。");
          while (session.paused && !session.cancelled) {
            await new Promise((resolve) => window.setTimeout(resolve, 100));
          }
          if (socket.readyState !== WebSocket.OPEN) throw new Error("连接已关闭。");
          await new Promise((resolve) => window.setTimeout(resolve, 30));
        }
        const chunk = await file.slice(offset, offset + chunkSize).arrayBuffer();
        while (session.paused && !session.cancelled) {
          await new Promise((resolve) => window.setTimeout(resolve, 100));
        }
        if (session.cancelled) throw new Error("文件传输已取消。");
        if (socket.readyState !== WebSocket.OPEN) throw new Error("连接已关闭。");
        socket.send(chunk);
        offset += chunk.byteLength;
        const progressBucket = Math.floor(offset / (5 * 1024 * 1024));
        if (progressBucket > loggedProgressBucket) {
          loggedProgressBucket = progressBucket;
          recordDiagnosticEvent("file_transfer_progress", offset, file.size);
        }
        const active = activeFileTransferRef.current;
        if (active?.transferId === transferId) {
          updateActiveFileTransfer({ ...active, transferred: offset });
        }
        setTransferStatus(`正在发送 ${file.name}：${offset} / ${file.size} 字节`);
      }
      socket.send(JSON.stringify({ type: "file_complete", transfer_id: transferId }));
      setTransferStatus(`数据已发送，正在校验并保存 ${file.name}…`);
    } catch (cause) {
      pendingFileSendsRef.current.delete(transferId);
      const detail = cause instanceof Error ? cause.message : String(cause);
      if (detail === "文件传输已取消。") {
        updateActiveFileTransfer(null);
        setTransferStatus(`已取消 ${file.name}。`);
        return;
      }
      updateActiveFileTransfer(null);
      recordDiagnosticEvent("file_transfer_failed");
      setTransferStatus(`文件发送失败：${detail}。续传标识已保留；重连后再次选择同一文件可继续。`);
    }
  }

  async function selectAgentFile(file: File | undefined) {
    if (!file || !agentConnected) return;
    if (file.size === 0 || file.size > 100 * 1024 * 1024) {
      setAgentClipboardStatus("文件必须大于 0 且不超过 100 MiB。");
      return;
    }
    if (pendingAgentFileRef.current) {
      setAgentClipboardStatus("已有文件正在等待发送或传输，请完成后再试。");
      return;
    }
    try {
      setAgentClipboardStatus(`正在校验 ${file.name}，请稍候…`);
      const sha256 = await hashFile(file);
      const targetIdentity = agentPeerIdentityRef.current || "unknown-controller";
      const transferId = await getOrCreateFileTransferId(file.name, sha256, targetIdentity);
      if (!agentConnected) throw new Error("校验完成时被控端连接已断开。");
      pendingAgentFileRef.current = { transferId, file, sha256, paused: false, cancelled: false };
      recordDiagnosticEvent("file_transfer_started", file.size);
      updateActiveFileTransfer({
        transferId,
        fileName: file.name,
        size: file.size,
        transferred: 0,
        paused: false,
        path: "tauri",
        mode: "send",
      });
      await invoke("begin_agent_file_transfer", {
        transferId,
        fileName: file.name,
        size: file.size,
        sha256,
      });
      setAgentClipboardStatus(`已请求发送 ${file.name}，等待控制端批准。`);
    } catch (cause) {
      pendingAgentFileRef.current = null;
      updateActiveFileTransfer(null);
      const detail = cause instanceof Error ? cause.message : String(cause);
      setAgentClipboardStatus(`无法发送文件：${detail}`);
    }
  }

  async function transmitAgentFile(transferId: string, file: File, resumeOffset: number) {
    let offset = resumeOffset;
    let loggedProgressBucket = Math.floor(resumeOffset / (5 * 1024 * 1024));
    let awaitingCompletion = false;
    try {
      const chunkSize = 48 * 1024;
      while (offset < file.size) {
        const session = pendingAgentFileRef.current;
        if (!session || session.transferId !== transferId) throw new Error("文件传输已取消。");
        while (session.paused && !session.cancelled) {
          await new Promise((resolve) => window.setTimeout(resolve, 100));
        }
        if (session.cancelled) throw new Error("文件传输已取消。");
        const chunk = new Uint8Array(await file.slice(offset, offset + chunkSize).arrayBuffer());
        while (session.paused && !session.cancelled) {
          await new Promise((resolve) => window.setTimeout(resolve, 100));
        }
        if (session.cancelled) throw new Error("文件传输已取消。");
        await invoke("send_agent_file_chunk", {
          transferId,
          bytes: Array.from(chunk),
        });
        offset += chunk.byteLength;
        const progressBucket = Math.floor(offset / (5 * 1024 * 1024));
        if (progressBucket > loggedProgressBucket) {
          loggedProgressBucket = progressBucket;
          recordDiagnosticEvent("file_transfer_progress", offset, file.size);
        }
        const active = activeFileTransferRef.current;
        if (active?.transferId === transferId) {
          updateActiveFileTransfer({ ...active, transferred: offset });
        }
        setAgentClipboardStatus(`正在发送 ${file.name}：${offset} / ${file.size} 字节`);
      }
      await invoke("finish_agent_file_transfer", { transferId });
      awaitingCompletion = true;
      setAgentClipboardStatus(`数据已发送，正在校验并保存 ${file.name}…`);
    } catch (cause) {
      const detail = cause instanceof Error ? cause.message : String(cause);
      if (detail === "文件传输已取消。") {
        setAgentClipboardStatus(`已取消 ${file.name}。`);
      } else {
        recordDiagnosticEvent("file_transfer_failed");
        setAgentClipboardStatus(`文件发送失败：${detail}。续传标识已保留；连接恢复后再次选择同一文件可继续。`);
      }
    } finally {
      if (!awaitingCompletion) {
        pendingAgentFileRef.current = null;
        updateActiveFileTransfer(null);
      }
    }
  }

  async function finishIncomingAgentFile(socket: WebSocket, transferId: string) {
    const incoming = incomingFileTransferRef.current;
    if (!incoming || incoming.transferId !== transferId || incoming.receivedBytes !== incoming.size) {
      incomingFileTransferRef.current = null;
      if (incoming) {
        suspendControllerFileReceive(incoming.transferId, incoming.writeQueue);
      }
      notifyFileTransferError(socket, transferId, "文件数据未完整，续传进度已保留。");
      setTransferStatus("文件传输未完整，无法保存。");
      return;
    }
    incomingFileTransferRef.current = null;
    try {
      await incoming.writeQueue;
      if (incoming.writeError) throw new Error(incoming.writeError);
      const savedPath = await invoke<string>("finish_controller_file_receive", { transferId });
      socket.send(JSON.stringify({ type: "file_transfer_complete", transfer_id: transferId }));
      updateActiveFileTransfer(null);
      recordDiagnosticEvent("file_transfer_completed", incoming.size);
      setTransferStatus(`已接收 ${incoming.fileName}，保存至 ${savedPath}`);
    } catch (cause) {
      suspendControllerFileReceive(transferId);
      updateActiveFileTransfer(null);
      const detail = cause instanceof Error ? cause.message : String(cause);
      recordDiagnosticEvent("file_transfer_failed");
      setTransferStatus(`接收文件失败：${detail}。已接收数据已保留，可重新连接后续传。`);
      notifyFileTransferError(socket, transferId, detail);
    }
  }

  function cancelControllerFileReceive(transferId: string, writeQueue: Promise<void> = Promise.resolve()) {
    void writeQueue.then(() => invoke("cancel_controller_file_receive", { transferId }))
      .catch((cause: unknown) => {
        const detail = cause instanceof Error ? cause.message : String(cause);
        setTransferStatus(`无法清理未完成文件：${detail}`);
      });
  }

  function suspendControllerFileReceive(transferId: string, writeQueue: Promise<void> = Promise.resolve()) {
    void writeQueue.then(() => invoke("suspend_controller_file_receive", { transferId }))
      .catch((cause: unknown) => {
        const detail = cause instanceof Error ? cause.message : String(cause);
        setTransferStatus(`无法保存文件续传进度：${detail}`);
      });
  }

  function notifyFileTransferError(socket: WebSocket, transferId: string, message: string) {
    if (socket.readyState !== WebSocket.OPEN) return;
    try {
      socket.send(JSON.stringify({ type: "file_transfer_error", transfer_id: transferId, message }));
    } catch (cause) {
      const detail = cause instanceof Error ? cause.message : String(cause);
      setTransferStatus(`无法通知对端文件接收失败：${detail}`);
    }
  }

  async function controlActiveFileTransfer(action: "pause" | "resume" | "cancel") {
    const active = activeFileTransferRef.current;
    if (!active) return;
    const session = active.path === "websocket"
      ? pendingFileSendsRef.current.get(active.transferId)
      : pendingAgentFileRef.current?.transferId === active.transferId
        ? pendingAgentFileRef.current
        : null;
    if (session && action !== "cancel") session.paused = action === "pause";
    if (session?.cancelled) return;
    if (session && action === "cancel") session.cancelled = true;

    try {
      if (active.path === "websocket") {
        const socket = connectionRef.current;
        if (!socket || socket.readyState !== WebSocket.OPEN) throw new Error("控制连接已关闭。");
        socket.send(JSON.stringify({
          type: "file_transfer_control",
          transfer_id: active.transferId,
          action,
        }));
      } else {
        await invoke("control_agent_file_transfer", {
          transferId: active.transferId,
          action,
        });
      }
      const event = action === "pause"
        ? "file_transfer_paused"
        : action === "resume"
          ? "file_transfer_resumed"
          : "file_transfer_cancelled";
      recordDiagnosticEvent(event, active.transferred, active.size);

      if (action === "cancel") {
        if (active.mode === "send") {
          const sendingSession = active.path === "websocket"
            ? pendingFileSendsRef.current.get(active.transferId)
            : pendingAgentFileRef.current?.transferId === active.transferId
              ? pendingAgentFileRef.current
              : null;
          if (sendingSession) clearFileTransferIdentity(active.transferId);
          pendingFileSendsRef.current.delete(active.transferId);
          if (pendingAgentFileRef.current?.transferId === active.transferId) {
            pendingAgentFileRef.current = null;
          }
        } else if (active.path === "websocket") {
          const incoming = incomingFileTransferRef.current;
          if (incoming?.transferId === active.transferId) {
            incomingFileTransferRef.current = null;
            cancelControllerFileReceive(active.transferId, incoming.writeQueue);
          }
        }
        updateActiveFileTransfer(null);
        setTransferStatus(`已取消 ${active.fileName} 的文件传输。`);
      } else {
        updateActiveFileTransfer({ ...active, paused: action === "pause" });
        setTransferStatus(action === "pause" ? `已暂停 ${active.fileName}。` : `已继续 ${active.fileName}。`);
      }
    } catch (cause) {
      if (session) session.paused = false;
      const detail = cause instanceof Error ? cause.message : String(cause);
      setTransferStatus(`无法${action === "pause" ? "暂停" : action === "resume" ? "继续" : "取消"}文件传输：${detail}`);
    }
  }

  function sendInput(message: Record<string, string | number | boolean>) {
    const channel = controlChannelRef.current;
    if (channel?.readyState === "open" && channel.bufferedAmount < 512_000) {
      channel.send(JSON.stringify(message));
    }
  }

  function restoreRemoteFocus() {
    window.requestAnimationFrame(() => {
      if (controlChannelRef.current?.readyState === "open") {
        viewerRef.current?.focus({ preventScroll: true });
      }
    });
  }

  function sendMouseMove(event: React.PointerEvent<HTMLDivElement>) {
    if (event.pointerType !== "mouse") return;
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

  function mouseButtonName(button: number) {
    if (button === 0) return "left";
    if (button === 1) return "middle";
    if (button === 2) return "right";
    return null;
  }

  function sendMouseButton(event: React.PointerEvent<HTMLDivElement>, down: boolean) {
    if (event.pointerType !== "mouse") return;
    const button = mouseButtonName(event.button);
    if (!button) return;
    event.preventDefault();
    event.currentTarget.focus({ preventScroll: true });
    if (down) {
      pressedRemoteMouseButtonsRef.current.add(button);
      event.currentTarget.setPointerCapture(event.pointerId);
    } else {
      pressedRemoteMouseButtonsRef.current.delete(button);
    }
    sendInput({ type: "mouse_button", button, down });
  }

  function releaseRemoteMouseButtons() {
    for (const button of pressedRemoteMouseButtonsRef.current) {
      sendInput({ type: "mouse_button", button, down: false });
    }
    pressedRemoteMouseButtonsRef.current.clear();
  }

  function releaseRemoteInput() {
    pressedRemoteMouseButtonsRef.current.clear();
    sendInput({ type: "release_all" });
  }

  return (
    <main className={`app-shell${standaloneDeviceId ? " standalone-mode" : ""}${isNativeFullscreen ? " native-remote-fullscreen" : ""}`}>
      <aside className="sidebar">
        <div className="brand" aria-label="Lantern">
          <span className="brand-mark" aria-hidden="true">
            <img alt="" src="/lantern-icon.svg" />
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
          {standaloneDeviceId && (
            <div className="remote-window-heading">
              <div>
                <div className="eyebrow"><span /> REMOTE DESKTOP</div>
                <h1>{devices.find((device) => device.id === standaloneDeviceId)?.name ?? "远程控制"}</h1>
              </div>
              <div className="remote-window-actions">
                <span className="remote-window-status">
                  <span className={connectionStatus === "connected" ? "lan-status-dot" : "offline-dot"} />
                  {connectionStatus === "connected" ? "已连接" : connectionStatus === "waitingApproval" ? "等待被控端批准" : connectionStatus === "error" ? "连接失败" : connectionStatus === "idle" ? "未连接" : "正在连接"}
                </span>
                <button
                  className="secondary-button"
                  onClick={() => disconnectFromDevice()}
                  type="button"
                >
                  <Icon name="close" />
                  关闭窗口
                </button>
              </div>
            </div>
          )}
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
              {agentConnected && (
                <>
                  <button className="text-button agent-clipboard-button" onClick={() => void sendAgentClipboard()} type="button">
                    发送本机剪贴板
                  </button>
                  <button className="text-button agent-clipboard-button" onClick={() => agentFileInputRef.current?.click()} type="button">
                    发送文件到控制端
                  </button>
                  <input
                    ref={agentFileInputRef}
                    className="visually-hidden"
                    type="file"
                    onChange={(event) => {
                      void selectAgentFile(event.currentTarget.files?.[0]);
                      event.currentTarget.value = "";
                    }}
                  />
                </>
              )}
            </div>
          )}
          {agentClipboardStatus && <div className="transfer-status agent-clipboard-status" role="status">{agentClipboardStatus}</div>}
          {activeFileTransfer && (
            <div className="file-transfer-controls" role="group" aria-label="文件传输控制">
              <div className="file-transfer-progress">
                <strong>{activeFileTransfer.fileName}</strong>
                <span>
                  {activeFileTransfer.paused ? "已暂停" : activeFileTransfer.mode === "send" ? "正在发送" : "正在接收"}
                  {" · "}
                  {activeFileTransfer.transferred.toLocaleString()} / {activeFileTransfer.size.toLocaleString()} 字节
                </span>
                <progress value={activeFileTransfer.transferred} max={activeFileTransfer.size} />
              </div>
              <button
                className="secondary-button"
                onClick={() => void controlActiveFileTransfer(activeFileTransfer.paused ? "resume" : "pause")}
                type="button"
              >
                {activeFileTransfer.paused ? "继续" : "暂停"}
              </button>
              <button
                className="transfer-cancel-button"
                onClick={() => void controlActiveFileTransfer("cancel")}
                type="button"
              >
                取消传输
              </button>
            </div>
          )}
          {connectionStatus === "error" && connectionError && (
            <div className="connection-error" role="alert">
              <span>{connectionError}</span>
              {activeDevice && (
                <button
                  className="text-button"
                  onClick={() => connectToDevice(activeDevice)}
                  type="button"
                >
                  重试连接
                </button>
              )}
              <button className="text-button" onClick={() => disconnectFromDevice()} type="button">关闭</button>
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
              {devices.map((device, index) => {
                const standaloneStatus = standaloneWindowStatuses[device.id];
                const standaloneStatusLabel = standaloneStatus === "opening"
                  ? "正在打开独立窗口"
                  : standaloneStatus === "connected"
                    ? "独立窗口已连接"
                    : standaloneStatus === "waitingApproval"
                      ? "独立窗口等待批准"
                      : standaloneStatus === "connecting"
                        ? "独立窗口连接中"
                        : standaloneStatus === "error"
                          ? "独立窗口连接失败"
                          : standaloneStatus === "idle"
                            ? "独立窗口未连接"
                            : "";
                return (
                <article
                  className={`device-card${device.id === activeId ? " card-active" : ""}${device.id === standaloneDeviceId ? " standalone-device-card" : ""}`}
                  key={device.id}
                >
                  {connectionDeviceId === device.id && connectionStatus === "connected" && !standaloneStatus ? (
                    <div
                      className={`preview-area remote-preview${standaloneDeviceId ? "" : " remote-preview-readonly"}`}
                      ref={viewerRef}
                      role={standaloneDeviceId ? "application" : "region"}
                      aria-label={standaloneDeviceId
                        ? `${device.name} 远程桌面，可使用鼠标和键盘控制`
                        : `${device.name} 远程桌面预览`}
                      tabIndex={standaloneDeviceId ? 0 : -1}
                      onContextMenu={(event) => event.preventDefault()}
                      onPointerCancel={standaloneDeviceId ? releaseRemoteMouseButtons : undefined}
                      onPointerDown={standaloneDeviceId ? (event) => sendMouseButton(event, true) : undefined}
                      onPointerMove={standaloneDeviceId ? sendMouseMove : undefined}
                      onPointerUp={standaloneDeviceId ? (event) => sendMouseButton(event, false) : undefined}
                      onBlur={standaloneDeviceId ? releaseRemoteInput : undefined}
                      onWheel={(event) => {
                        event.preventDefault();
                        if (!standaloneDeviceId) return;
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
                      <audio
                        autoPlay
                        className="remote-audio"
                        playsInline
                        ref={remoteAudioRef}
                        onPlaying={() => setAudioStatus("远端音频播放已启动。")}
                        onError={(event) => {
                          const code = event.currentTarget.error?.code;
                          setAudioStatus(`浏览器音频输出失败${code ? `（错误码 ${code}）` : ""}。`);
                        }}
                      />
                      {playRemoteSystemAudio && audioStatus && (
                        <div
                          className="remote-audio-status"
                          onPointerDown={(event) => {
                            event.preventDefault();
                            event.stopPropagation();
                          }}
                        >
                          <span>{audioStatus}</span>
                            {audioTransportStatus && <span>{audioTransportStatus}</span>}
                            {remoteAudioStreamRef.current && (
                              <button onClick={playRemoteAudio} type="button">
                                {audioPlaybackBlocked ? "点击播放" : "重新启动声音"}
                              </button>
                            )}
                        </div>
                      )}
                      <span className="remote-live">
                        <span className="lan-status-dot" />
                        WebRTC · {encoderStatus || "编码器检测中"} · {standaloneDeviceId ? "鼠标和键盘可控" : "只读预览"}
                        {isRemoteFullscreen && keyboardLockWarning && (
                          <span className="fullscreen-key-warning" title={keyboardLockWarning}>{keyboardLockWarning}</span>
                        )}
                      </span>
                      {standaloneDeviceId ? <div
                        className="remote-toolbar"
                        onPointerDown={(event) => event.stopPropagation()}
                        onPointerMove={(event) => event.stopPropagation()}
                        onPointerUp={(event) => event.stopPropagation()}
                      >
                        <button
                          className="icon-button remote-toolbar-button"
                          type="button"
                          aria-label="发送剪贴板"
                          title="发送本机剪贴板"
                          onPointerDown={(event) => {
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
                          onPointerDown={(event) => {
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
                          onPointerDown={(event) => {
                            event.preventDefault();
                            event.stopPropagation();
                          }}
                          onClick={() => void toggleRemoteFullscreen()}
                        >
                          <Icon name={isRemoteFullscreen ? "minimize" : "maximize"} />
                        </button>
                      </div> : (
                        <button
                          className="remote-preview-open-control"
                          type="button"
                          onClick={() => void openRemoteWindow(device)}
                        >
                          在独立窗口中控制
                        </button>
                      )}
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
                      {standaloneStatus && (
                        <span className="standalone-preview-overlay">
                          <strong>已在独立窗口打开</strong>
                          <span>{standaloneStatusLabel}</span>
                        </span>
                      )}
                    </button>
                  )}
                  <input
                    ref={fileInputRef}
                    className="visually-hidden"
                    type="file"
                    onChange={(event) => {
                      void selectFile(event.currentTarget.files?.[0]).finally(restoreRemoteFocus);
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
                    {standaloneStatus && !standaloneDeviceId ? (
                      <button
                        className="connect-button connect-button-muted"
                        type="button"
                        onClick={() => void closeRemoteWindow(device.id)}
                      >
                        {standaloneStatus === "idle" ? "关闭窗口" : "断开"}
                      </button>
                    ) : deviceLayout === "list" && !standaloneDeviceId ? (
                      <button
                        className="connect-button"
                        type="button"
                        onClick={() => void openRemoteWindow(device)}
                      >
                        打开控制窗口
                      </button>
                    ) : connectionDeviceId === device.id && connectionStatus !== "idle" ? (
                      <button
                        className="connect-button connect-button-muted"
                        type="button"
                        onClick={() => disconnectFromDevice()}
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
                            className="menu-edit"
                            type="button"
                            role="menuitem"
                            onClick={() => {
                              setOpenDeviceMenuId(null);
                              setEditingDevice(device);
                              setName(device.name);
                              setAddress(device.address);
                              setError("");
                            }}
                          >
                            编辑设备信息…
                          </button>
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
                );
              })}
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

      {(isAdding || editingDevice) && (
        <div
          className="modal-backdrop"
          onMouseDown={(event) => {
            if (event.target === event.currentTarget) closeDeviceForm();
          }}
        >
          <form className="device-dialog" onSubmit={saveDevice}>
            <button
              className="dialog-close icon-button"
              type="button"
              aria-label="关闭"
              onClick={closeDeviceForm}
            >
              <Icon name="close" />
            </button>
            <span className="dialog-icon"><Icon name="desktop" /></span>
            <h2>{editingDevice ? "编辑设备信息" : "添加局域网设备"}</h2>
            <p>输入远程电脑的名称和局域网地址。两台设备需处于同一网络。</p>
            {!editingDevice && (
              <div className="device-discovery">
                <button
                  className="secondary-button"
                  disabled={isDiscovering}
                  onClick={() => void discoverDevices()}
                  type="button"
                >
                  {isDiscovering ? "正在扫描…" : "扫描局域网设备"}
                </button>
                {discoveryStatus && <p role="status">{discoveryStatus}</p>}
                {discoveredDevices.length > 0 && (
                  <div className="discovered-device-list">
                    {discoveredDevices.map((device) => (
                      <button
                        key={device.address}
                        onClick={() => {
                          setName(device.name);
                          setAddress(device.address);
                          setError("");
                        }}
                        type="button"
                      >
                        <strong>{device.name}</strong>
                        <span>{device.address}</span>
                      </button>
                    ))}
                  </div>
                )}
              </div>
            )}
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
                onClick={closeDeviceForm}
                type="button"
              >
                取消
              </button>
              <button className="primary-button" disabled={isSavingDevice} type="submit">
                {isSavingDevice ? "正在校验…" : editingDevice ? "保存修改" : "保存设备"}
              </button>
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
          <section className="settings-dialog settings-layout" aria-labelledby="settings-title">
            <nav className="settings-sidebar" aria-label="设置栏目">
              <h2 id="settings-title">应用设置</h2>
              <div className="settings-navigation">
                {([
                  ["video", "画面与控制"],
                  ["audio", "声音"],
                  ["clipboard", "剪贴板"],
                  ["window", "窗口行为"],
                  ["trusted", "信任设备"],
                  ["diagnostics", "诊断日志"],
                ] as const).map(([section, label]) => (
                  <button
                    key={section}
                    className={settingsSection === section ? "settings-navigation-item selected" : "settings-navigation-item"}
                    type="button"
                    aria-current={settingsSection === section ? "page" : undefined}
                    onClick={() => {
                      setSettingsSection(section);
                      if (section === "diagnostics") void refreshDiagnosticLog();
                    }}
                  >
                    {label}
                  </button>
                ))}
              </div>
              <button
                className="secondary-button settings-close-button"
                onClick={() => setShowSettings(false)}
                type="button"
              >
                完成
              </button>
            </nav>
            <div className="settings-page">
              <button
                className="dialog-close icon-button"
                type="button"
                aria-label="关闭设置"
                onClick={() => setShowSettings(false)}
              >
                <Icon name="close" />
              </button>
              {settingsSection === "video" && (
                <div className="settings-page-content">
                  <h3>画面与控制</h3>
                  <p>显示器和画质设置会在下一次连接时应用到被控端。</p>
                  <label className="settings-select-label" htmlFor="capture-display">被控端显示器</label>
                  <select
                    className="settings-select"
                    id="capture-display"
                    value={selectedDisplayId ?? ""}
                    onChange={(event) => {
                      const value = event.currentTarget.value;
                      setSelectedDisplayId(value === "" ? null : Number(value));
                    }}
                  >
                    <option value="">自动（主显示器）</option>
                    {availableDisplays.map((display) => (
                      <option key={display.id} value={display.id}>
                        {display.name} · {display.width} × {display.height}
                      </option>
                    ))}
                  </select>
                  {displaySettingsError && <p className="form-error" role="alert">{displaySettingsError}</p>}
                  {!isTauri() && <p className="settings-note">显示器选择仅在 Lantern 桌面应用中可用。</p>}
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
                  <section className="settings-guidance" aria-labelledby="permission-guidance-title">
                    <h4 id="permission-guidance-title">系统权限提示</h4>
                    <p><strong>macOS：</strong>需要为 Lantern 开启“屏幕录制”和“辅助功能”权限；修改权限后可能需要重启应用。</p>
                    <p><strong>Windows：</strong>远程控制受 UAC 安全桌面和权限级别限制；建议两端应用以相同权限运行。</p>
                    <p><strong>Linux：</strong>屏幕采集和键鼠控制取决于桌面会话；Wayland 支持因桌面环境而异，遇到问题可尝试 X11 会话。</p>
                  </section>
                </div>
              )}
              {settingsSection === "clipboard" && (
                <div className="settings-page-content">
                  <h3>剪贴板</h3>
                  <p>自动同步仅传输文本，两端均需启用。</p>
                  <label className="clipboard-setting settings-clipboard-setting">
                    <input
                      checked={clipboardEnabled}
                      disabled={clipboardSettingLocked}
                      onChange={(event) => setClipboardEnabled(event.currentTarget.checked)}
                      type="checkbox"
                    />
                    <span>
                      <strong>启用双向自动剪贴板同步</strong>
                      <small>接收方首次确认后，本次连接内后续内容自动写入；断开后重新询问。仍可使用远程工具栏手动发送。</small>
                    </span>
                  </label>
                  {clipboardSettingLocked && (
                    <p className="settings-note">连接或被控服务运行期间无法修改此选项，请先断开连接并停止被控端。</p>
                  )}
                </div>
              )}
              {settingsSection === "audio" && (
                <div className="settings-page-content">
                  <h3>系统声音</h3>
                  <p>仅将被控端的系统输出声音单向传到控制端，不共享麦克风。默认关闭；播放和分享分别控制。</p>
                  <label className="clipboard-setting settings-clipboard-setting">
                    <input
                      checked={playRemoteSystemAudio}
                      disabled={playRemoteAudioSettingLocked || !isTauri()}
                      onChange={(event) => setPlayRemoteSystemAudio(event.currentTarget.checked)}
                      type="checkbox"
                    />
                    <span>
                      <strong>在本机播放远端系统声音</strong>
                      <small>此选项仅控制端需要开启，下一次连接时生效。</small>
                    </span>
                  </label>
                  <label className="clipboard-setting settings-clipboard-setting">
                    <input
                      checked={shareSystemAudio}
                      disabled={shareSystemAudioSettingLocked || !isTauri()}
                      onChange={(event) => setShareSystemAudio(event.currentTarget.checked)}
                      type="checkbox"
                    />
                    <span>
                      <strong>允许本机系统声音共享</strong>
                      <small>此选项仅被控端需要开启。仅在控制端请求声音且本项开启时采集，不会共享麦克风。</small>
                    </span>
                  </label>
                  {audioStatus && <p className="settings-note" role="status">{audioStatus}</p>}
                  {audioTransportStatus && <p className="settings-note" role="status">{audioTransportStatus}</p>}
                  {!isTauri() && <p className="settings-note">系统声音传输仅在 Lantern 桌面应用中可用。</p>}
                  {playRemoteAudioSettingLocked && (
                    <p className="settings-note">控制端连接期间无法修改播放选项，请先断开连接。</p>
                  )}
                  {shareSystemAudioSettingLocked && (
                    <p className="settings-note">被控端连接请求或会话进行期间无法修改共享选项。</p>
                  )}
                  <section className="permission-notes">
                    <p><strong>macOS：</strong>需要 macOS 14.4 或更新版本，并在首次提示时允许音频采集；macOS 13–14.3 仍可远程控制，但不支持系统声音传输。</p>
                    <p><strong>Windows：</strong>若系统隐私设置阻止音频捕获，请在 Windows 隐私设置中允许 Lantern。</p>
                    <p><strong>Linux：</strong>系统声音捕获需要正在运行的 PipeWire 会话。</p>
                  </section>
                </div>
              )}
              {settingsSection === "window" && (
                <div className="settings-page-content">
                  <h3>窗口行为</h3>
                  <p>设置关闭主窗口时 Lantern 的行为。</p>
                  <fieldset className="quality-options">
                    <legend>关闭主窗口时</legend>
                    <label className={closeToTray ? "quality-option selected" : "quality-option"}>
                      <input
                        checked={closeToTray}
                        disabled={savingCloseToTray}
                        name="close-behavior"
                        onChange={() => void updateCloseToTray(true)}
                        type="radio"
                        value="tray"
                      />
                      <span>
                        <strong>最小化到系统托盘</strong>
                        <small>远程服务继续运行，可从托盘菜单重新打开或退出。</small>
                      </span>
                    </label>
                    <label className={!closeToTray ? "quality-option selected" : "quality-option"}>
                      <input
                        checked={!closeToTray}
                        disabled={savingCloseToTray}
                        name="close-behavior"
                        onChange={() => void updateCloseToTray(false)}
                        type="radio"
                        value="quit"
                      />
                      <span>
                        <strong>彻底退出应用</strong>
                        <small>关闭窗口时退出 Lantern，并停止被控端服务。</small>
                      </span>
                    </label>
                    {closeToTrayError && <p className="form-error" role="alert">{closeToTrayError}</p>}
                  </fieldset>
                </div>
              )}
              {settingsSection === "trusted" && (
                <div className="settings-page-content">
                  <h3>信任设备</h3>
                  <p>信任后，同一 IP 的连接请求将自动获批。仅信任你控制的设备。</p>
                  <section className="trusted-devices-section">
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
                </div>
              )}
              {settingsSection === "diagnostics" && (
                <div className="settings-page-content">
                  <h3>诊断日志</h3>
                  <p>记录连接、媒体协商、音视频帧与 RTP 统计、传输控制和发现状态；仅附带有限数值计数，不记录设备地址、文件名、按键值、剪贴板内容或屏幕内容。日志保存在本机。</p>
                  {diagnosticLogError && <p className="form-error" role="alert">{diagnosticLogError}</p>}
                  {!isTauri() ? (
                    <p className="settings-note">诊断日志仅在 Lantern 桌面应用中可用。</p>
                  ) : (
                    <>
                      <div className="diagnostic-actions">
                        <button className="secondary-button" onClick={() => void refreshDiagnosticLog()} type="button">
                          刷新
                        </button>
                        <button
                          className="secondary-button"
                          disabled={!diagnosticLog}
                          onClick={() => void copyDiagnosticLog()}
                          type="button"
                        >
                          复制日志
                        </button>
                      </div>
                      <pre className="diagnostic-log" aria-label="本机诊断日志">
                        {diagnosticLog || "暂无诊断记录。"}
                      </pre>
                    </>
                  )}
                </div>
              )}
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
              {pendingFileOffer.receivedBytes > 0
                ? ` 已接收 ${pendingFileOffer.receivedBytes.toLocaleString()} 字节，接受后将从断点继续。`
                : ""}
              接受后将保存到“下载/Lantern Received”目录。
            </p>
            <div className="dialog-actions">
              <button className="secondary-button" onClick={() => void answerFileOffer(false)} type="button">拒绝</button>
              <button className="primary-button" onClick={() => void answerFileOffer(true)} type="button">
                {pendingFileOffer.receivedBytes > 0 ? "继续接收" : "接受文件"}
              </button>
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
