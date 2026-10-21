use std::{
    collections::HashSet,
    fs::{self, File, OpenOptions},
    io::Write,
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

use arboard::Clipboard;
use axum::{
    Router,
    extract::{
        ConnectInfo, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::IntoResponse,
    routing::get,
};
use enigo::{Axis, Button, Coordinate, Direction, Enigo, Key, Keyboard, Mouse, Settings};
use image::imageops::FilterType;
use reactor_webrtc::{
    IceCandidate, MediaKind, PeerConnection, PeerConnectionFactory, PeerConnectionObserver,
    PeerConnectionState, RtcConfiguration, SdpType, SessionDescription, StreamKind, VideoCodec,
    VideoFrame,
};
use serde::{Deserialize, Serialize};
use tauri::Manager;
use tauri::{AppHandle, Emitter};
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::timeout,
};
use uuid::Uuid;

const DEFAULT_PORT: u16 = 4200;
const APPROVAL_TIMEOUT: Duration = Duration::from_secs(60);
const SIGNALING_TIMEOUT: Duration = Duration::from_secs(20);
const MAX_FILE_TRANSFER_SIZE: u64 = 100 * 1024 * 1024;
const MAX_CLIPBOARD_TEXT_SIZE: usize = 48 * 1024;
const MAX_FILE_CHUNK_SIZE: usize = 48 * 1024;

#[derive(Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum VideoQuality {
    Quality,
    #[default]
    Balanced,
    Smooth,
}

#[tauri::command]
pub fn set_clipboard_sync_enabled(remote: tauri::State<'_, Arc<RemoteState>>, enabled: bool) {
    remote
        .clipboard_sync_enabled
        .store(enabled, Ordering::Release);
}

struct ClipboardWatcher {
    stopping: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

impl ClipboardWatcher {
    fn start(
        app: AppHandle,
        remote: Arc<RemoteState>,
        sender: mpsc::UnboundedSender<String>,
    ) -> Self {
        let stopping = Arc::new(AtomicBool::new(false));
        let stopping_for_thread = Arc::clone(&stopping);
        let thread = thread::spawn(move || {
            let mut clipboard = match Clipboard::new() {
                Ok(clipboard) => clipboard,
                Err(error) => {
                    let _ = app.emit("clipboard-status", format!("无法监视系统剪贴板：{error}"));
                    return;
                }
            };
            let mut watching = false;
            while !stopping_for_thread.load(Ordering::Acquire) {
                thread::sleep(Duration::from_millis(400));
                if !remote.clipboard_sync_enabled.load(Ordering::Acquire) {
                    watching = false;
                    continue;
                }
                let Ok(text) = clipboard.get_text() else {
                    continue;
                };
                if !watching {
                    if let Ok(mut last_shared) = remote.clipboard_last_text.lock() {
                        *last_shared = Some(text);
                    }
                    watching = true;
                    continue;
                }
                let changed = match remote.clipboard_last_text.lock() {
                    Ok(mut last_shared) if last_shared.as_ref() != Some(&text) => {
                        *last_shared = Some(text.clone());
                        true
                    }
                    Ok(_) => false,
                    Err(_) => {
                        let _ = app.emit("clipboard-status", "剪贴板同步状态不可用");
                        return;
                    }
                };
                if !changed {
                    continue;
                }
                if text.len() <= MAX_CLIPBOARD_TEXT_SIZE && sender.send(text).is_err() {
                    return;
                }
            }
        });
        Self {
            stopping,
            thread: Some(thread),
        }
    }
}

impl Drop for ClipboardWatcher {
    fn drop(&mut self) {
        self.stopping.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[tauri::command]
pub fn send_agent_clipboard(
    remote: tauri::State<'_, Arc<RemoteState>>,
    text: String,
) -> Result<(), String> {
    if text.len() > MAX_CLIPBOARD_TEXT_SIZE {
        return Err("剪贴板内容超过 48 KiB，未发送。".to_string());
    }
    let sender = remote
        .clipboard_sender
        .lock()
        .map_err(|_| "剪贴板发送状态不可用".to_string())?
        .clone()
        .ok_or_else(|| "控制端未启用剪贴板共享，或当前没有可用连接。".to_string())?;
    sender
        .send(text)
        .map_err(|_| "剪贴板发送通道已关闭，请重新连接后重试。".to_string())
}

#[tauri::command]
pub fn get_system_clipboard_text() -> Result<String, String> {
    let mut clipboard = Clipboard::new().map_err(|error| format!("无法访问系统剪贴板：{error}"))?;
    clipboard
        .get_text()
        .map_err(|error| format!("无法读取系统剪贴板：{error}"))
}

#[tauri::command]
pub fn set_system_clipboard_text(
    remote: tauri::State<'_, Arc<RemoteState>>,
    text: String,
) -> Result<(), String> {
    if text.len() > MAX_CLIPBOARD_TEXT_SIZE {
        return Err("剪贴板内容超过 48 KiB，未写入。".to_string());
    }
    let mut clipboard = Clipboard::new().map_err(|error| format!("无法访问系统剪贴板：{error}"))?;
    let mut last_text = remote
        .clipboard_last_text
        .lock()
        .map_err(|_| "剪贴板同步状态不可用".to_string())?;
    clipboard
        .set_text(text.clone())
        .map_err(|error| format!("无法写入系统剪贴板：{error}"))?;
    *last_text = Some(text);
    Ok(())
}

#[tauri::command]
pub fn begin_agent_file_transfer(
    remote: tauri::State<'_, Arc<RemoteState>>,
    transfer_id: String,
    file_name: String,
    size: u64,
) -> Result<(), String> {
    if transfer_id.is_empty()
        || transfer_id.len() > 64
        || file_name.is_empty()
        || file_name.len() > 255
        || PathBuf::from(&file_name)
            .file_name()
            .and_then(|name| name.to_str())
            != Some(file_name.as_str())
        || size == 0
        || size > MAX_FILE_TRANSFER_SIZE
    {
        return Err("文件名或大小无效，文件不能超过 100 MiB。".to_string());
    }
    let sender = remote
        .file_sender
        .lock()
        .map_err(|_| "文件传输状态不可用".to_string())?
        .clone()
        .ok_or_else(|| "当前没有可用的控制连接。".to_string())?;
    sender
        .send(AgentFileCommand::Offer {
            transfer_id: transfer_id.clone(),
            file_name,
            size,
        })
        .map_err(|_| "文件传输通道已关闭，请重新连接后重试。".to_string())?;
    Ok(())
}

#[tauri::command]
pub fn send_agent_file_chunk(
    remote: tauri::State<'_, Arc<RemoteState>>,
    transfer_id: String,
    bytes: Vec<u8>,
) -> Result<(), String> {
    if transfer_id.len() > 64 || bytes.is_empty() || bytes.len() > MAX_FILE_CHUNK_SIZE {
        return Err("文件数据块无效。".to_string());
    }
    let sender = remote
        .file_sender
        .lock()
        .map_err(|_| "文件传输状态不可用".to_string())?
        .clone()
        .ok_or_else(|| "当前没有可用的控制连接。".to_string())?;
    sender
        .send(AgentFileCommand::Chunk { transfer_id, bytes })
        .map_err(|_| "文件传输通道已关闭，请重新连接后重试。".to_string())
}

#[tauri::command]
pub fn finish_agent_file_transfer(
    remote: tauri::State<'_, Arc<RemoteState>>,
    transfer_id: String,
) -> Result<(), String> {
    let sender = remote
        .file_sender
        .lock()
        .map_err(|_| "文件传输状态不可用".to_string())?
        .clone()
        .ok_or_else(|| "当前没有可用的控制连接。".to_string())?;
    sender
        .send(AgentFileCommand::Complete { transfer_id })
        .map_err(|_| "文件传输通道已关闭，请重新连接后重试。".to_string())
}

#[tauri::command]
pub fn begin_controller_file_receive(
    app: AppHandle,
    remote: tauri::State<'_, Arc<RemoteState>>,
    transfer_id: String,
    file_name: String,
    size: u64,
) -> Result<(), String> {
    if transfer_id.is_empty()
        || transfer_id.len() > 64
        || file_name.is_empty()
        || file_name.len() > 255
        || size == 0
        || size > MAX_FILE_TRANSFER_SIZE
    {
        return Err("文件接收请求无效或超过 100 MiB 限制".to_string());
    }
    let mut pending = remote
        .controller_file_receiver
        .lock()
        .map_err(|_| "文件接收状态不可用".to_string())?;
    if pending.is_some() {
        return Err("已有文件正在接收。".to_string());
    }
    let (file, path) = create_received_file(&app, &file_name)?;
    *pending = Some(ControllerFileReceiver {
        transfer_id,
        file,
        path,
        expected_size: size,
        received_size: 0,
    });
    Ok(())
}

#[tauri::command]
pub fn append_controller_file_chunk(
    remote: tauri::State<'_, Arc<RemoteState>>,
    transfer_id: String,
    bytes: Vec<u8>,
) -> Result<(), String> {
    if bytes.is_empty() || bytes.len() > MAX_FILE_CHUNK_SIZE {
        return Err("文件数据块无效。".to_string());
    }
    let mut pending = remote
        .controller_file_receiver
        .lock()
        .map_err(|_| "文件接收状态不可用".to_string())?;
    let receiver = pending
        .as_mut()
        .filter(|receiver| receiver.transfer_id == transfer_id)
        .ok_or_else(|| "文件接收请求已过期。".to_string())?;
    let next_size = receiver.received_size.saturating_add(bytes.len() as u64);
    if next_size > receiver.expected_size {
        return Err("收到的文件数据超过声明大小。".to_string());
    }
    receiver
        .file
        .write_all(&bytes)
        .map_err(|error| format!("写入接收文件失败：{error}"))?;
    receiver.received_size = next_size;
    Ok(())
}

#[tauri::command]
pub fn finish_controller_file_receive(
    remote: tauri::State<'_, Arc<RemoteState>>,
    transfer_id: String,
) -> Result<String, String> {
    let mut pending = remote
        .controller_file_receiver
        .lock()
        .map_err(|_| "文件接收状态不可用".to_string())?;
    let receiver = pending
        .take()
        .filter(|receiver| receiver.transfer_id == transfer_id)
        .ok_or_else(|| "文件接收请求已过期。".to_string())?;
    let path = receiver.path;
    if receiver.received_size != receiver.expected_size {
        drop(receiver.file);
        let _ = fs::remove_file(&path);
        return Err("文件数据未完整，无法保存。".to_string());
    }
    if let Err(error) = receiver.file.sync_all() {
        drop(receiver.file);
        let _ = fs::remove_file(&path);
        return Err(format!("无法完成接收文件写入：{error}"));
    }
    Ok(path.to_string_lossy().into_owned())
}

#[tauri::command]
pub fn cancel_controller_file_receive(
    remote: tauri::State<'_, Arc<RemoteState>>,
    transfer_id: String,
) -> Result<(), String> {
    let mut pending = remote
        .controller_file_receiver
        .lock()
        .map_err(|_| "文件接收状态不可用".to_string())?;
    if pending
        .as_ref()
        .is_some_and(|receiver| receiver.transfer_id == transfer_id)
        && let Some(receiver) = pending.take()
    {
        let path = receiver.path;
        drop(receiver.file);
        fs::remove_file(path).map_err(|error| format!("无法清理未完成的接收文件：{error}"))?;
    }
    Ok(())
}

impl VideoQuality {
    fn settings(self) -> VideoSettings {
        match self {
            Self::Quality => VideoSettings {
                max_width: 2560,
                frame_interval: Duration::from_millis(33),
                min_bitrate: 8_000_000,
                max_bitrate: 20_000_000,
            },
            Self::Balanced => VideoSettings {
                max_width: 1920,
                frame_interval: Duration::from_millis(33),
                min_bitrate: 4_000_000,
                max_bitrate: 12_000_000,
            },
            Self::Smooth => VideoSettings {
                max_width: 1600,
                frame_interval: Duration::from_millis(16),
                min_bitrate: 3_000_000,
                max_bitrate: 12_000_000,
            },
        }
    }
}

#[derive(Clone, Copy)]
struct VideoSettings {
    max_width: u32,
    frame_interval: Duration,
    min_bitrate: i32,
    max_bitrate: i32,
}

#[derive(Default)]
pub struct RemoteState {
    server: Mutex<Option<JoinHandle<()>>>,
    pending_approval: Mutex<Option<(String, oneshot::Sender<bool>)>>,
    pending_request: Mutex<Option<ConnectionRequest>>,
    pending_file_approval: Mutex<Option<(String, oneshot::Sender<bool>)>>,
    clipboard_sender: Mutex<Option<mpsc::UnboundedSender<String>>>,
    file_sender: Mutex<Option<mpsc::UnboundedSender<AgentFileCommand>>>,
    clipboard_last_text: Mutex<Option<String>>,
    clipboard_sync_enabled: AtomicBool,
    controller_file_receiver: Mutex<Option<ControllerFileReceiver>>,
    webrtc_factory: Mutex<Option<Arc<PeerConnectionFactory>>>,
    active_connection: AtomicBool,
    stopping: AtomicBool,
}

enum AgentFileCommand {
    Offer {
        transfer_id: String,
        file_name: String,
        size: u64,
    },
    Chunk {
        transfer_id: String,
        bytes: Vec<u8>,
    },
    Complete {
        transfer_id: String,
    },
}

struct ControllerFileReceiver {
    transfer_id: String,
    file: File,
    path: PathBuf,
    expected_size: u64,
    received_size: u64,
}

struct AgentIncomingFileTransfer {
    transfer_id: String,
    file_name: String,
    expected_size: u64,
    contents: Vec<u8>,
    accepted: bool,
    _approval_guard: PendingFileApprovalGuard,
}

struct PendingFileApprovalGuard {
    remote: Arc<RemoteState>,
    transfer_id: String,
}

impl Drop for PendingFileApprovalGuard {
    fn drop(&mut self) {
        if let Ok(mut pending) = self.remote.pending_file_approval.lock()
            && pending
                .as_ref()
                .is_some_and(|(transfer_id, _)| transfer_id == &self.transfer_id)
        {
            pending.take();
        }
    }
}

struct AgentFileSenderGuard(Arc<RemoteState>);

impl Drop for AgentFileSenderGuard {
    fn drop(&mut self) {
        if let Ok(mut sender) = self.0.file_sender.lock() {
            sender.take();
        }
    }
}

fn local_hostname() -> Result<String, String> {
    #[cfg(target_os = "windows")]
    let hostname =
        std::env::var("COMPUTERNAME").map_err(|error| format!("无法读取本机名称：{error}"))?;

    #[cfg(not(target_os = "windows"))]
    let hostname = {
        let output = std::process::Command::new("hostname")
            .output()
            .map_err(|error| format!("无法读取本机名称：{error}"))?;
        if !output.status.success() {
            return Err(format!(
                "读取本机名称失败，hostname 退出状态：{}",
                output.status
            ));
        }
        String::from_utf8(output.stdout)
            .map_err(|error| format!("本机名称不是有效 UTF-8：{error}"))?
    };

    let hostname = hostname.trim();
    if hostname.is_empty() {
        return Err("本机名称为空，请检查系统设备名称设置。".to_string());
    }
    Ok(hostname.to_string())
}

#[derive(Clone)]
struct ServerState {
    app: AppHandle,
    remote: Arc<RemoteState>,
    webrtc_factory: Arc<PeerConnectionFactory>,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionRequest {
    request_id: String,
    peer_address: String,
    controller_name: String,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ClientMessage {
    Hello {
        device_name: String,
        #[serde(default)]
        quality: VideoQuality,
        #[serde(default)]
        clipboard_enabled: bool,
    },
    Offer {
        sdp: String,
    },
    IceCandidate {
        candidate: String,
        #[serde(rename = "sdpMid")]
        sdp_mid: Option<String>,
        #[serde(rename = "sdpMLineIndex")]
        sdp_mline_index: Option<u16>,
    },
    MouseMove {
        x: f64,
        y: f64,
    },
    MouseButton {
        button: String,
        down: bool,
    },
    MouseScroll {
        delta_y: f64,
    },
    Key {
        code: String,
        key: String,
        down: bool,
    },
    ReleaseAll,
    ClipboardText {
        text: String,
    },
    FileTransferComplete {
        transfer_id: String,
    },
    FileOffer {
        transfer_id: String,
        file_name: String,
        size: u64,
    },
    FileComplete {
        transfer_id: String,
    },
    FileTransferDecision {
        transfer_id: String,
        accepted: bool,
    },
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LocalDeviceInfo {
    name: String,
    address: String,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FileTransferOffer {
    transfer_id: String,
    file_name: String,
    size: u64,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClipboardOffer {
    text: String,
}

struct ClipboardSenderGuard(Arc<RemoteState>);

impl Drop for ClipboardSenderGuard {
    fn drop(&mut self) {
        if let Ok(mut sender) = self.0.clipboard_sender.lock() {
            sender.take();
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TrustedDevice {
    ip_address: String,
}

fn trusted_devices_path(app: &AppHandle) -> Result<PathBuf, String> {
    let mut path = app
        .path()
        .app_local_data_dir()
        .map_err(|error| format!("无法定位信任设备存储目录：{error}"))?;
    path.push("trusted-devices.json");
    Ok(path)
}

fn read_trusted_ips(app: &AppHandle) -> Result<HashSet<IpAddr>, String> {
    let path = trusted_devices_path(app)?;
    let contents = match fs::read_to_string(&path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(HashSet::new()),
        Err(error) => return Err(format!("无法读取信任设备列表：{error}")),
    };
    let values: Vec<String> = serde_json::from_str(&contents)
        .map_err(|error| format!("信任设备列表格式无效：{error}"))?;
    Ok(values
        .into_iter()
        .filter_map(|value| value.parse::<IpAddr>().ok())
        .collect())
}

fn write_trusted_ips(app: &AppHandle, ips: &HashSet<IpAddr>) -> Result<(), String> {
    let path = trusted_devices_path(app)?;
    let directory = path
        .parent()
        .ok_or_else(|| "信任设备存储路径无效".to_string())?;
    fs::create_dir_all(directory).map_err(|error| format!("无法创建信任设备存储目录：{error}"))?;
    let mut values: Vec<String> = ips.iter().map(ToString::to_string).collect();
    values.sort();
    let contents = serde_json::to_vec_pretty(&values)
        .map_err(|error| format!("无法序列化信任设备列表：{error}"))?;
    let temporary_path = path.with_extension(format!("{}.tmp", Uuid::new_v4()));
    fs::write(&temporary_path, contents)
        .map_err(|error| format!("无法保存信任设备列表：{error}"))?;
    #[cfg(windows)]
    if path.exists() {
        fs::remove_file(&path).map_err(|error| format!("无法更新信任设备列表：{error}"))?;
    }
    fs::rename(&temporary_path, &path).map_err(|error| format!("无法应用信任设备列表：{error}"))
}

#[tauri::command]
pub fn get_trusted_devices(app: AppHandle) -> Result<Vec<TrustedDevice>, String> {
    let mut ips: Vec<String> = read_trusted_ips(&app)?
        .into_iter()
        .map(|ip| ip.to_string())
        .collect();
    ips.sort();
    Ok(ips
        .into_iter()
        .map(|ip_address| TrustedDevice { ip_address })
        .collect())
}

#[tauri::command]
pub fn remove_trusted_device(app: AppHandle, ip_address: String) -> Result<(), String> {
    let ip = ip_address
        .parse::<IpAddr>()
        .map_err(|_| "信任设备 IP 地址格式无效".to_string())?;
    let mut ips = read_trusted_ips(&app)?;
    ips.remove(&ip);
    write_trusted_ips(&app, &ips)
}

#[tauri::command]
pub fn answer_file_transfer(
    remote: tauri::State<'_, Arc<RemoteState>>,
    transfer_id: String,
    accepted: bool,
) -> Result<(), String> {
    let mut pending = remote
        .pending_file_approval
        .lock()
        .map_err(|_| "文件传输授权状态不可用".to_string())?;
    match pending.take() {
        Some((pending_id, sender)) if pending_id == transfer_id => {
            let _ = sender.send(accepted);
            Ok(())
        }
        Some(request) => {
            *pending = Some(request);
            Err("文件传输请求已过期".to_string())
        }
        None => Err("当前没有待处理的文件传输请求".to_string()),
    }
}

#[tauri::command]
pub fn get_local_device_info() -> Result<LocalDeviceInfo, String> {
    let name = local_hostname()?;
    let address = local_lan_address()
        .map(|ip| format!("{ip}:{DEFAULT_PORT}"))
        .ok_or_else(|| "无法确定本机局域网 IP，请检查网络连接。".to_string())?;
    Ok(LocalDeviceInfo { name, address })
}

#[tauri::command]
pub async fn start_agent(
    app: AppHandle,
    remote: tauri::State<'_, Arc<RemoteState>>,
    port: Option<u16>,
) -> Result<String, String> {
    {
        let server = remote
            .server
            .lock()
            .map_err(|_| "远程服务状态不可用".to_string())?;
        if server.is_some() {
            return Ok(format!(
                "已在端口 {} 等待连接",
                port.unwrap_or(DEFAULT_PORT)
            ));
        }
    }
    if remote.stopping.load(Ordering::Acquire) && remote.active_connection.load(Ordering::Acquire) {
        return Err("正在关闭上一条控制连接，请稍后重试".to_string());
    }

    let existing_factory = remote
        .webrtc_factory
        .lock()
        .map_err(|_| "WebRTC 状态不可用".to_string())?
        .clone();
    let webrtc_factory = if let Some(factory) = existing_factory {
        factory
    } else {
        let factory = tokio::task::spawn_blocking(|| {
            PeerConnectionFactory::builder()
                .build()
                .map(Arc::new)
                .map_err(|error| format!("无法初始化 WebRTC：{error}"))
        })
        .await
        .map_err(|error| format!("WebRTC 初始化任务失败：{error}"))??;
        let mut stored_factory = remote
            .webrtc_factory
            .lock()
            .map_err(|_| "WebRTC 状态不可用".to_string())?;
        Arc::clone(stored_factory.get_or_insert(factory))
    };

    let port = port.unwrap_or(DEFAULT_PORT);
    let listener = TcpListener::bind(SocketAddr::from(([0, 0, 0, 0], port)))
        .await
        .map_err(|error| format!("无法监听端口 {port}：{error}"))?;
    let address = local_lan_address()
        .map(|ip| format!("{ip}:{port}"))
        .unwrap_or_else(|| format!("本机局域网地址:{port}"));
    let state = ServerState {
        app: app.clone(),
        remote: Arc::clone(&remote),
        webrtc_factory,
    };
    remote.stopping.store(false, Ordering::Release);
    let app_for_status = app.clone();

    let handle = tokio::spawn(async move {
        let service = Router::new()
            .route("/ws", get(websocket_upgrade))
            .with_state(state)
            .into_make_service_with_connect_info::<SocketAddr>();
        let result = axum::serve(listener, service).await;
        let message = match result {
            Ok(()) => "远程服务已停止".to_string(),
            Err(error) => format!("远程服务异常：{error}"),
        };
        let _ = app_for_status.emit("agent-status", message);
    });
    let mut server = remote
        .server
        .lock()
        .map_err(|_| "远程服务状态不可用".to_string())?;
    if server.is_some() {
        handle.abort();
        return Err("远程服务已在另一个请求中启动".to_string());
    }
    *server = Some(handle);

    Ok(address)
}

#[tauri::command]
pub fn stop_agent(remote: tauri::State<'_, Arc<RemoteState>>) -> Result<(), String> {
    if let Some(server) = remote
        .server
        .lock()
        .map_err(|_| "远程服务状态不可用".to_string())?
        .take()
    {
        remote.stopping.store(true, Ordering::Release);
        server.abort();
    }
    if let Some((_, approval)) = remote
        .pending_approval
        .lock()
        .map_err(|_| "连接授权状态不可用".to_string())?
        .take()
    {
        let _ = approval.send(false);
    }
    if let Some((_, approval)) = remote
        .pending_file_approval
        .lock()
        .map_err(|_| "文件传输授权状态不可用".to_string())?
        .take()
    {
        let _ = approval.send(false);
    }
    remote
        .pending_request
        .lock()
        .map_err(|_| "连接请求状态不可用".to_string())?
        .take();
    Ok(())
}

#[tauri::command]
pub fn get_pending_connection_request(
    remote: tauri::State<'_, Arc<RemoteState>>,
) -> Result<Option<ConnectionRequest>, String> {
    remote
        .pending_request
        .lock()
        .map(|request| request.clone())
        .map_err(|_| "连接请求状态不可用".to_string())
}

#[tauri::command]
pub fn answer_connection_request(
    app: AppHandle,
    remote: tauri::State<'_, Arc<RemoteState>>,
    request_id: String,
    approved: bool,
    trust_device: bool,
) -> Result<(), String> {
    let mut pending = remote
        .pending_approval
        .lock()
        .map_err(|_| "连接授权状态不可用".to_string())?;
    match pending.take() {
        Some((pending_id, sender)) if pending_id == request_id => {
            let request = remote
                .pending_request
                .lock()
                .map_err(|_| "连接请求状态不可用".to_string())?
                .take();
            if approved && trust_device {
                let request = request.ok_or_else(|| "连接请求已过期".to_string())?;
                let ip = request
                    .peer_address
                    .parse::<SocketAddr>()
                    .map_err(|_| "控制端地址格式无效，无法加入信任列表".to_string())?
                    .ip();
                let mut ips = read_trusted_ips(&app)?;
                ips.insert(ip);
                write_trusted_ips(&app, &ips)?;
            }
            let _ = sender.send(approved);
            Ok(())
        }
        Some(request) => {
            *pending = Some(request);
            Err("连接请求已过期".to_string())
        }
        None => Err("当前没有待处理的连接请求".to_string()),
    }
}

async fn websocket_upgrade(
    ws: WebSocketUpgrade,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    State(state): State<ServerState>,
) -> impl IntoResponse {
    if !is_lan_address(peer.ip()) {
        return axum::http::StatusCode::FORBIDDEN.into_response();
    }
    ws.max_message_size(512 * 1024)
        .max_frame_size(512 * 1024)
        .on_upgrade(move |socket| handle_connection(socket, peer, state))
        .into_response()
}

async fn handle_connection(mut socket: WebSocket, peer: SocketAddr, state: ServerState) {
    if state
        .remote
        .active_connection
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        let _ = socket
            .send(Message::Text(
                serde_json::json!({"type": "error", "message": "已有控制端连接"})
                    .to_string()
                    .into(),
            ))
            .await;
        return;
    }

    let (controller_name, video_quality, clipboard_enabled) =
        match timeout(Duration::from_secs(5), socket.recv()).await {
            Ok(Some(Ok(Message::Text(text)))) => match serde_json::from_str::<ClientMessage>(&text)
            {
                Ok(ClientMessage::Hello {
                    device_name,
                    quality,
                    clipboard_enabled,
                }) => {
                    let name: String = device_name.trim().chars().take(48).collect();
                    let name = if name.is_empty() {
                        "局域网控制端".to_string()
                    } else {
                        name
                    };
                    (name, quality, clipboard_enabled)
                }
                _ => {
                    let _ = socket
                        .send(Message::Text(
                            serde_json::json!({"type": "error", "message": "无效的连接握手"})
                                .to_string()
                                .into(),
                        ))
                        .await;
                    state
                        .remote
                        .active_connection
                        .store(false, Ordering::Release);
                    return;
                }
            },
            _ => {
                state
                    .remote
                    .active_connection
                    .store(false, Ordering::Release);
                return;
            }
        };

    let request_id = Uuid::new_v4().to_string();
    let request = ConnectionRequest {
        request_id: request_id.clone(),
        peer_address: peer.to_string(),
        controller_name: controller_name.clone(),
    };
    let trusted = match read_trusted_ips(&state.app) {
        Ok(ips) => ips.contains(&peer.ip()),
        Err(error) => {
            let _ = state.app.emit("agent-status", error);
            false
        }
    };
    let approved = if trusted {
        true
    } else {
        let (approval_sender, mut approval_receiver) = oneshot::channel();
        {
            let mut pending = match state.remote.pending_approval.lock() {
                Ok(pending) => pending,
                Err(_) => {
                    state
                        .remote
                        .active_connection
                        .store(false, Ordering::Release);
                    return;
                }
            };
            *pending = Some((request_id.clone(), approval_sender));
        }
        match state.remote.pending_request.lock() {
            Ok(mut pending) => *pending = Some(request.clone()),
            Err(_) => {
                clear_pending(&state.remote, &request_id);
                state
                    .remote
                    .active_connection
                    .store(false, Ordering::Release);
                return;
            }
        }
        if state
            .app
            .emit("remote-connection-request", request)
            .is_err()
        {
            clear_pending(&state.remote, &request_id);
            state
                .remote
                .active_connection
                .store(false, Ordering::Release);
            return;
        }

        let approval_deadline = tokio::time::sleep(APPROVAL_TIMEOUT);
        tokio::pin!(approval_deadline);
        let approved = loop {
            tokio::select! {
                result = &mut approval_receiver => break result.unwrap_or(false),
                incoming = socket.recv() => {
                    match incoming {
                        Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break false,
                        Some(Ok(Message::Ping(payload))) => {
                            if socket.send(Message::Pong(payload)).await.is_err() {
                                break false;
                            }
                        }
                        Some(Ok(_)) => {}
                    }
                }
                _ = &mut approval_deadline => break false,
            }
        };
        clear_pending(&state.remote, &request_id);
        let _ = state
            .app
            .emit("remote-connection-request-ended", request_id.clone());
        approved
    };
    if !approved {
        let _ = socket
            .send(Message::Text(
                serde_json::json!({"type": "error", "message": "连接未获批准"})
                    .to_string()
                    .into(),
            ))
            .await;
        state
            .remote
            .active_connection
            .store(false, Ordering::Release);
        return;
    }

    if send_signal(&mut socket, serde_json::json!({"type": "approved"}))
        .await
        .is_err()
    {
        state
            .remote
            .active_connection
            .store(false, Ordering::Release);
        return;
    }

    let session_error = match run_webrtc_session(
        &mut socket,
        &state,
        video_quality,
        clipboard_enabled,
        &controller_name,
    )
    .await
    {
        Ok(()) => None,
        Err(error) => {
            let _ = send_signal(
                &mut socket,
                serde_json::json!({"type": "error", "message": error}),
            )
            .await;
            let _ = socket.send(Message::Close(None)).await;
            Some(error)
        }
    };

    let _ = state.app.emit("agent-connection-state", false);
    state
        .remote
        .active_connection
        .store(false, Ordering::Release);
    let _ = state.app.emit(
        "agent-status",
        session_error.unwrap_or_else(|| "控制端已断开".to_string()),
    );
}

enum WebRtcSignal {
    Candidate(IceCandidate),
    State(PeerConnectionState),
    CaptureError(String),
    EncoderStatus {
        codec: Option<String>,
        encoder: Option<String>,
        hardware: Option<bool>,
    },
}

struct StatsWorker {
    stopping: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

impl StatsWorker {
    fn new() -> Self {
        Self {
            stopping: Arc::new(AtomicBool::new(false)),
            thread: None,
        }
    }

    fn start(
        &mut self,
        peer_connection: Arc<PeerConnection>,
        signals: mpsc::UnboundedSender<WebRtcSignal>,
    ) {
        if self.thread.is_some() {
            return;
        }
        let stopping = Arc::clone(&self.stopping);
        self.thread = Some(thread::spawn(move || {
            while !stopping.load(Ordering::Acquire) {
                thread::sleep(Duration::from_secs(1));
                if stopping.load(Ordering::Acquire) {
                    break;
                }
                if let Ok(stats) = peer_connection.get_stats()
                    && let Some(video) = stats
                        .outbound_rtp
                        .iter()
                        .find(|stream| stream.kind == StreamKind::Video)
                {
                    let _ = signals.send(WebRtcSignal::EncoderStatus {
                        codec: video.codec_mime_type.clone(),
                        encoder: video.encoder_implementation.clone(),
                        hardware: video.power_efficient_encoder,
                    });
                }
            }
        }));
    }
}

impl Drop for StatsWorker {
    fn drop(&mut self) {
        self.stopping.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

struct CaptureWorker {
    stopping: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

impl CaptureWorker {
    fn new() -> Self {
        Self {
            stopping: Arc::new(AtomicBool::new(false)),
            thread: None,
        }
    }

    fn start(
        &mut self,
        track: Arc<reactor_webrtc::VideoTrack>,
        signals: mpsc::UnboundedSender<WebRtcSignal>,
        settings: VideoSettings,
    ) {
        if self.thread.is_some() {
            return;
        }
        let stopping = Arc::clone(&self.stopping);
        self.thread = Some(thread::spawn(move || {
            while !stopping.load(Ordering::Acquire) {
                let started = std::time::Instant::now();
                match capture_bgra(settings.max_width) {
                    Ok((frame, frame_width, frame_height)) => {
                        if let Err(error) =
                            track.push_frame(VideoFrame::new(&frame, frame_width, frame_height))
                        {
                            let _ = signals.send(WebRtcSignal::CaptureError(error.to_string()));
                            break;
                        }
                    }
                    Err(error) => {
                        let _ = signals.send(WebRtcSignal::CaptureError(error));
                        break;
                    }
                }
                thread::sleep(settings.frame_interval.saturating_sub(started.elapsed()));
            }
        }));
    }
}

impl Drop for CaptureWorker {
    fn drop(&mut self) {
        self.stopping.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

async fn run_webrtc_session(
    socket: &mut WebSocket,
    state: &ServerState,
    quality: VideoQuality,
    clipboard_enabled: bool,
    controller_name: &str,
) -> Result<(), String> {
    let offer = match timeout(SIGNALING_TIMEOUT, async {
        loop {
            match socket.recv().await {
                Some(Ok(Message::Text(text))) => {
                    return match serde_json::from_str::<ClientMessage>(&text) {
                        Ok(ClientMessage::Offer { sdp }) => Ok(SessionDescription {
                            kind: SdpType::Offer,
                            sdp,
                        }),
                        _ => Err("等待控制端 WebRTC offer 时收到无效消息".to_string()),
                    };
                }
                Some(Ok(Message::Ping(payload))) => socket
                    .send(Message::Pong(payload))
                    .await
                    .map_err(|error| format!("WebSocket 错误：{error}"))?,
                Some(Ok(Message::Close(_))) | None => {
                    return Err("控制端在建立 WebRTC 连接前已断开".to_string());
                }
                Some(Err(error)) => return Err(format!("WebSocket 错误：{error}")),
                Some(Ok(_)) => return Err("等待控制端 WebRTC offer 时收到无效消息".to_string()),
            }
        }
    })
    .await
    {
        Ok(result) => result?,
        Err(_) => return Err("等待控制端 WebRTC offer 超时".to_string()),
    };

    let (width, height) = tokio::task::spawn_blocking(capture_dimensions)
        .await
        .map_err(|error| format!("屏幕尺寸读取任务失败：{error}"))??;
    let (input_sender, input_receiver) = std::sync::mpsc::channel::<ClientMessage>();
    let (ready_sender, ready_receiver) = std::sync::mpsc::sync_channel(1);
    let (signal_sender, mut signal_receiver) = mpsc::unbounded_channel::<WebRtcSignal>();
    let (input_error_sender, mut input_error_receiver) = mpsc::unbounded_channel::<String>();
    let channel_error_sender = input_error_sender.clone();
    let data_channels = Arc::new(Mutex::new(Vec::new()));

    thread::spawn(move || {
        let mut enigo = match Enigo::new(&Settings::default()) {
            Ok(enigo) => {
                let _ = ready_sender.send(Ok(()));
                enigo
            }
            Err(error) => {
                let _ = ready_sender.send(Err(error.to_string()));
                return;
            }
        };
        let mut input_state = InputState::default();
        for command in input_receiver {
            if let Err(error) = apply_input(&mut enigo, command, width, height, &mut input_state) {
                let _ = input_error_sender.send(error);
                break;
            }
        }
        for key in input_state.pressed_keys {
            let _ = enigo.key(key, Direction::Release);
        }
        for button in input_state.pressed_buttons {
            let _ = enigo.button(button, Direction::Release);
        }
        #[cfg(target_os = "windows")]
        for scancode in input_state.pressed_scancodes {
            let _ = enigo.raw(scancode, Direction::Release);
        }
    });

    match ready_receiver.recv() {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            return Err(format!(
                "无法启用键鼠控制：{error}，请检查被控端的辅助功能权限。"
            ));
        }
        Err(error) => return Err(format!("键鼠控制服务启动失败：{error}")),
    }

    let input_for_channel = input_sender.clone();
    let channels_for_peer = Arc::clone(&data_channels);
    let signals_for_peer = signal_sender.clone();
    let signals_for_state = signal_sender.clone();
    let observer = PeerConnectionObserver::new()
        .on_ice_candidate(move |candidate| {
            let _ = signals_for_peer.send(WebRtcSignal::Candidate(candidate));
        })
        .on_connection_state_change(move |connection_state| {
            let _ = signals_for_state.send(WebRtcSignal::State(connection_state));
        })
        .on_data_channel(move |channel| {
            let input_for_this_channel = input_for_channel.clone();
            channel.on_message(move |data, binary| {
                if binary {
                    return;
                }
                if let Ok(text) = std::str::from_utf8(data)
                    && let Ok(command) = serde_json::from_str::<ClientMessage>(text)
                    && is_input_message(&command)
                {
                    let _ = input_for_this_channel.send(command);
                }
            });
            match channels_for_peer.lock() {
                Ok(mut channels) => channels.push(channel),
                Err(_) => {
                    let _ = channel_error_sender.send("WebRTC 键鼠控制通道状态不可用".to_string());
                }
            }
        });

    let rtc_configuration = RtcConfiguration {
        min_port: Some(4201),
        max_port: Some(4210),
        ..RtcConfiguration::default()
    };
    let peer_connection = Arc::new(
        state
            .webrtc_factory
            .create_peer_connection(&rtc_configuration, observer)
            .map_err(|error| format!("无法建立 WebRTC 连接：{error}"))?,
    );
    let video_settings = quality.settings();
    peer_connection
        .set_bitrate(
            None,
            Some(video_settings.min_bitrate),
            Some(video_settings.max_bitrate),
        )
        .map_err(|error| format!("无法配置 WebRTC 视频码率：{error}"))?;
    peer_connection
        .set_remote_description(&offer)
        .map_err(|error| format!("无法读取控制端 WebRTC offer：{error}"))?;

    let video_track = Arc::new(
        state
            .webrtc_factory
            .create_video_track("lantern-screen")
            .map_err(|error| format!("无法创建屏幕视频轨道：{error}"))?,
    );
    peer_connection
        .add_track(&video_track)
        .map_err(|error| format!("无法添加屏幕视频轨道：{error}"))?;
    for transceiver in peer_connection.transceivers() {
        if transceiver.kind() == MediaKind::Video {
            transceiver
                .set_codec_preferences(&[VideoCodec::H264, VideoCodec::Vp8])
                .map_err(|error| format!("无法配置 WebRTC 视频编码格式：{error}"))?;
        }
    }
    let answer = peer_connection
        .create_answer()
        .map_err(|error| format!("无法创建 WebRTC answer：{error}"))?;
    peer_connection
        .set_local_description(&answer)
        .map_err(|error| format!("无法应用 WebRTC answer：{error}"))?;
    send_signal(
        socket,
        serde_json::json!({"type": "answer", "sdp": answer.sdp}),
    )
    .await
    .map_err(|error| format!("无法发送 WebRTC answer：{error}"))?;

    let mut capture_worker = CaptureWorker::new();
    let mut stats_worker = StatsWorker::new();
    let (clipboard_sender, mut clipboard_receiver) = mpsc::unbounded_channel();
    let _clipboard_watcher = if clipboard_enabled {
        Some(ClipboardWatcher::start(
            state.app.clone(),
            Arc::clone(&state.remote),
            clipboard_sender.clone(),
        ))
    } else {
        None
    };
    if clipboard_enabled {
        *state
            .remote
            .clipboard_sender
            .lock()
            .map_err(|_| "剪贴板发送状态不可用".to_string())? = Some(clipboard_sender);
    }
    let _clipboard_sender_guard = ClipboardSenderGuard(Arc::clone(&state.remote));
    let (file_sender, mut file_receiver) = mpsc::unbounded_channel();
    *state
        .remote
        .file_sender
        .lock()
        .map_err(|_| "文件传输状态不可用".to_string())? = Some(file_sender);
    let _file_sender_guard = AgentFileSenderGuard(Arc::clone(&state.remote));
    let mut agent_file_transfer: Option<(String, u64, u64, bool)> = None;
    let mut agent_incoming_file_transfer: Option<AgentIncomingFileTransfer> = None;
    let mut file_approval_receiver: Option<oneshot::Receiver<bool>> = None;
    let mut file_approval_deadline = None;
    let mut file_receive_deadline = None;

    loop {
        tokio::select! {
            incoming = socket.recv() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        match serde_json::from_str::<ClientMessage>(&text) {
                            Ok(ClientMessage::IceCandidate { candidate, sdp_mid, sdp_mline_index }) => {
                                peer_connection
                                    .add_ice_candidate(&IceCandidate {
                                        candidate,
                                        sdp_mid,
                                        sdp_mline_index,
                                    })
                                    .map_err(|error| format!("无法添加控制端 ICE candidate：{error}"))?;
                            }
                            Ok(ClientMessage::Offer { .. }) => {
                                return Err("控制端重复发送 WebRTC offer".to_string());
                            }
                            Ok(ClientMessage::ClipboardText { text })
                                if clipboard_enabled
                                    && state
                                        .remote
                                        .clipboard_sync_enabled
                                        .load(Ordering::Acquire) =>
                            {
                                if text.len() <= MAX_CLIPBOARD_TEXT_SIZE {
                                    state.app.emit("remote-clipboard-offer", ClipboardOffer { text })
                                        .map_err(|error| format!("无法显示剪贴板接收请求：{error}"))?;
                                } else {
                                    send_signal(socket, serde_json::json!({
                                        "type": "clipboard_error",
                                        "message": "剪贴板内容超过 48 KiB，未发送。",
                                    }))
                                    .await
                                    .map_err(|error| format!("发送剪贴板错误失败：{error}"))?;
                                }
                            }
                            Ok(ClientMessage::ClipboardText { .. }) => {
                                send_signal(socket, serde_json::json!({
                                    "type": "clipboard_error",
                                    "message": "被控端未启用剪贴板共享。",
                                }))
                                .await
                                .map_err(|error| format!("发送剪贴板错误失败：{error}"))?;
                            }
                            Ok(ClientMessage::FileOffer {
                                transfer_id,
                                file_name,
                                size,
                            }) => {
                                let valid_offer = !transfer_id.is_empty()
                                    && transfer_id.len() <= 64
                                    && !file_name.is_empty()
                                    && file_name.len() <= 255
                                    && PathBuf::from(&file_name)
                                        .file_name()
                                        .and_then(|name| name.to_str())
                                        == Some(file_name.as_str())
                                    && size > 0
                                    && size <= MAX_FILE_TRANSFER_SIZE;
                                if !valid_offer
                                    || agent_file_transfer.is_some()
                                    || agent_incoming_file_transfer.is_some()
                                {
                                    send_signal(
                                        socket,
                                        serde_json::json!({
                                            "type": "file_transfer_decision",
                                            "transfer_id": transfer_id,
                                            "accepted": false,
                                        }),
                                    )
                                    .await
                                    .map_err(|error| format!("拒绝并发文件传输失败：{error}"))?;
                                } else {
                                    let (sender, receiver) = oneshot::channel();
                                    {
                                        let mut pending = state
                                            .remote
                                            .pending_file_approval
                                            .lock()
                                            .map_err(|_| "文件传输授权状态不可用".to_string())?;
                                        *pending = Some((transfer_id.clone(), sender));
                                    }
                                    let approval_guard = PendingFileApprovalGuard {
                                        remote: Arc::clone(&state.remote),
                                        transfer_id: transfer_id.clone(),
                                    };
                                    state
                                        .app
                                        .emit(
                                            "remote-file-offer",
                                            FileTransferOffer {
                                                transfer_id: transfer_id.clone(),
                                                file_name: file_name.clone(),
                                                size,
                                            },
                                        )
                                        .map_err(|error| {
                                            format!("无法显示文件接收请求：{error}")
                                        })?;
                                    agent_incoming_file_transfer =
                                        Some(AgentIncomingFileTransfer {
                                            transfer_id,
                                            file_name,
                                            expected_size: size,
                                            contents: Vec::with_capacity(size as usize),
                                            accepted: false,
                                            _approval_guard: approval_guard,
                                        });
                                    file_approval_receiver = Some(receiver);
                                    file_approval_deadline = Some(
                                        tokio::time::Instant::now()
                                            + Duration::from_secs(90),
                                    );
                                }
                            }
                            Ok(ClientMessage::FileComplete { transfer_id }) => {
                                let Some(incoming) = agent_incoming_file_transfer.take() else {
                                    return Err("收到未请求的文件完成标记".to_string());
                                };
                                if !incoming.accepted
                                    || incoming.transfer_id != transfer_id
                                    || incoming.contents.len() as u64 != incoming.expected_size
                                {
                                    return Err("文件传输完成标记与数据不匹配".to_string());
                                }
                                file_receive_deadline = None;
                                let saved_path =
                                    save_received_file(&state.app, &incoming.file_name, &incoming.contents)?;
                                send_signal(
                                    socket,
                                    serde_json::json!({"type": "file_transfer_complete"}),
                                )
                                .await
                                .map_err(|error| format!("发送文件完成状态失败：{error}"))?;
                                state
                                    .app
                                    .emit(
                                        "remote-file-received",
                                        serde_json::json!({
                                            "fileName": incoming.file_name,
                                            "savedPath": saved_path,
                                            "controllerName": controller_name,
                                        }),
                                    )
                                    .map_err(|error| {
                                        format!("无法显示文件保存状态：{error}")
                                    })?;
                            }
                            Ok(ClientMessage::FileTransferDecision { transfer_id, accepted }) => {
                                match agent_file_transfer.as_mut() {
                                    Some((active_id, _, _, is_accepted)) if *active_id == transfer_id => {
                                        *is_accepted = accepted;
                                        state.app.emit(
                                            "agent-file-transfer-decision",
                                            serde_json::json!({
                                                "transferId": transfer_id,
                                                "accepted": accepted,
                                            }),
                                        ).map_err(|error| format!("无法通知文件发送结果：{error}"))?;
                                        if !accepted {
                                            agent_file_transfer = None;
                                        }
                                    }
                                    _ => {
                                        send_signal(socket, serde_json::json!({
                                            "type": "file_transfer_error",
                                            "transfer_id": transfer_id,
                                            "message": "文件传输请求已过期。",
                                        }))
                                        .await
                                        .map_err(|error| format!("发送文件传输错误失败：{error}"))?;
                                    }
                                }
                            }
                            Ok(ClientMessage::FileTransferComplete { transfer_id }) => {
                                if agent_file_transfer.as_ref().is_some_and(
                                    |(active_id, size, sent, accepted)| {
                                        active_id == &transfer_id && *accepted && size == sent
                                    },
                                ) {
                                    agent_file_transfer = None;
                                    let _ = state.app.emit(
                                        "agent-file-transfer-status",
                                        "文件已成功送达控制端。",
                                    );
                                }
                            }
                            _ => return Err("信令通道收到无效消息".to_string()),
                        }
                    }
                    Some(Ok(Message::Binary(chunk))) => {
                        let Some(incoming) = agent_incoming_file_transfer
                            .as_mut()
                            .filter(|incoming| incoming.accepted)
                        else {
                            return Err("收到未请求的文件数据".to_string());
                        };
                        if chunk.is_empty() || chunk.len() > MAX_FILE_CHUNK_SIZE {
                            return Err("收到无效的文件数据块".to_string());
                        }
                        let next_size = incoming.contents.len().saturating_add(chunk.len());
                        if next_size as u64 > incoming.expected_size {
                            return Err("文件数据超过声明大小，传输已中止".to_string());
                        }
                        incoming.contents.extend_from_slice(&chunk);
                        file_receive_deadline = Some(
                            tokio::time::Instant::now() + Duration::from_secs(30),
                        );
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Err(error)) => return Err(format!("WebSocket 错误：{error}")),
                    Some(Ok(Message::Ping(payload))) => {
                        socket
                            .send(Message::Pong(payload))
                            .await
                            .map_err(|error| format!("WebSocket 错误：{error}"))?;
                    }

                    Some(Ok(_)) => {}
                }
            }
            decision = async {
                match file_approval_receiver.as_mut() {
                    Some(receiver) => receiver.await.ok(),
                    None => std::future::pending().await,
                }
            } => {
                file_approval_receiver = None;
                file_approval_deadline = None;
                if let Some(incoming) = agent_incoming_file_transfer.as_mut() {
                    let accepted = decision.unwrap_or(false);
                    incoming.accepted = accepted;
                    if accepted {
                        file_receive_deadline =
                            Some(tokio::time::Instant::now() + Duration::from_secs(30));
                    }
                    send_signal(
                        socket,
                        serde_json::json!({
                            "type": "file_transfer_decision",
                            "transfer_id": incoming.transfer_id,
                            "accepted": accepted,
                        }),
                    )
                    .await
                    .map_err(|error| format!("发送文件传输授权失败：{error}"))?;
                    if !accepted {
                        agent_incoming_file_transfer = None;
                    }
                }
            }
            _ = async {
                match file_approval_deadline {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => std::future::pending().await,
                }
            } => {
                file_approval_receiver = None;
                file_approval_deadline = None;
                if let Some(incoming) = agent_incoming_file_transfer.take() {
                    send_signal(
                        socket,
                        serde_json::json!({
                            "type": "file_transfer_decision",
                            "transfer_id": incoming.transfer_id,
                            "accepted": false,
                        }),
                    )
                    .await
                    .map_err(|error| format!("发送文件超时拒绝状态失败：{error}"))?;
                }
            }
            _ = async {
                match file_receive_deadline {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => std::future::pending().await,
                }
            } => {
                file_receive_deadline = None;
                if let Some(incoming) = agent_incoming_file_transfer.take() {
                    send_signal(socket, serde_json::json!({
                        "type": "file_transfer_error",
                        "transfer_id": incoming.transfer_id,
                        "message": "文件传输 30 秒无数据，已超时。",
                    }))
                    .await
                    .map_err(|error| format!("发送文件超时状态失败：{error}"))?;
                }
            }
            signal = signal_receiver.recv() => {
                match signal {
                    Some(WebRtcSignal::Candidate(candidate)) => {
                        send_signal(socket, serde_json::json!({
                            "type": "ice_candidate",
                            "candidate": candidate.candidate,
                            "sdpMid": candidate.sdp_mid,
                            "sdpMLineIndex": candidate.sdp_mline_index,
                        }))
                        .await
                        .map_err(|error| format!("发送 ICE candidate 失败：{error}"))?;
                    }
                    Some(WebRtcSignal::State(PeerConnectionState::Connected)) => {
                        send_signal(socket, serde_json::json!({"type": "connected"}))
                            .await
                            .map_err(|error| format!("发送连接状态失败：{error}"))?;
                        let _ = state.app.emit("agent-connection-state", true);
                        capture_worker.start(
                            Arc::clone(&video_track),
                            signal_sender.clone(),
                            video_settings,
                        );
                        stats_worker.start(Arc::clone(&peer_connection), signal_sender.clone());
                    }
                    Some(WebRtcSignal::State(PeerConnectionState::Failed | PeerConnectionState::Closed)) => {
                        return Err("WebRTC 媒体连接已失败或关闭".to_string());
                    }
                    Some(WebRtcSignal::State(PeerConnectionState::Disconnected)) => {
                        return Err("WebRTC 连接中断，请检查局域网和防火墙 UDP 设置。".to_string());
                    }
                    Some(WebRtcSignal::State(_)) => {}
                    Some(WebRtcSignal::CaptureError(error)) => {
                        return Err(format!("屏幕采集或视频编码失败：{error}"));
                    }
                    Some(WebRtcSignal::EncoderStatus { codec, encoder, hardware }) => {
                        let acceleration = match hardware {
                            Some(true) => "硬件加速",
                            Some(false) => "软件编码",
                            None => "加速状态未知",
                        };
                        let description = format!(
                            "{} · {} · {}",
                            codec.as_deref().unwrap_or("视频编码"),
                            encoder.as_deref().unwrap_or("编码器检测中"),
                            acceleration,
                        );
                        send_signal(
                            socket,
                            serde_json::json!({
                                "type": "encoder_status",
                                "status": description,
                            }),
                        )
                        .await
                        .map_err(|error| format!("发送编码器状态失败：{error}"))?;
                    }
                    None => return Err("WebRTC 信令通道已关闭".to_string()),
                }
            }
            text = clipboard_receiver.recv(), if clipboard_enabled => {
                if let Some(text) = text {
                    send_signal(socket, serde_json::json!({
                        "type": "clipboard_offer",
                        "text": text,
                    }))
                    .await
                    .map_err(|error| format!("发送剪贴板内容失败：{error}"))?;
                }
            }
            file_command = file_receiver.recv() => {
                match file_command {
                    Some(AgentFileCommand::Offer { transfer_id, file_name, size }) => {
                        if agent_file_transfer.is_some() || agent_incoming_file_transfer.is_some() {
                            state.app.emit("agent-file-transfer-decision", serde_json::json!({
                                "transferId": transfer_id,
                                "accepted": false,
                                "error": "已有文件正在传输。",
                            })).map_err(|error| format!("无法报告文件传输状态：{error}"))?;
                            continue;
                        }
                        send_signal(socket, serde_json::json!({
                            "type": "file_offer",
                            "transfer_id": transfer_id,
                            "file_name": file_name,
                            "size": size,
                        }))
                        .await
                        .map_err(|error| format!("发送文件请求失败：{error}"))?;
                        agent_file_transfer = Some((transfer_id, size, 0, false));
                    }
                    Some(AgentFileCommand::Chunk { transfer_id, bytes }) => {
                        match agent_file_transfer.as_mut() {
                            Some((active_id, size, sent, true)) if *active_id == transfer_id => {
                                let next_size = sent.saturating_add(bytes.len() as u64);
                                if next_size > *size {
                                    return Err("发送的文件数据超过声明大小".to_string());
                                }
                                socket
                                    .send(Message::Binary(bytes.into()))
                                    .await
                                    .map_err(|error| format!("发送文件数据失败：{error}"))?;
                                *sent = next_size;
                            }
                            _ => return Err("文件尚未获准传输或请求已过期".to_string()),
                        }
                    }
                    Some(AgentFileCommand::Complete { transfer_id }) => {
                        match agent_file_transfer.as_ref() {
                            Some((active_id, size, sent, true))
                                if active_id == &transfer_id && sent == size =>
                            {
                                send_signal(socket, serde_json::json!({
                                    "type": "file_complete",
                                    "transfer_id": transfer_id,
                                }))
                                .await
                                .map_err(|error| format!("发送文件完成信号失败：{error}"))?;
                                let _ = state.app.emit(
                                    "agent-file-transfer-status",
                                    format!("已发送文件，等待接收端完成保存。"),
                                );
                            }
                            _ => return Err("文件未完整发送，传输已取消".to_string()),
                        }
                    }
                    None => return Err("文件传输通道已关闭".to_string()),
                }
            }
            Some(error) = input_error_receiver.recv() => {
                return Err(format!("键鼠控制失败：{error}，请检查被控端的辅助功能权限。"));
            }
            _ = tokio::time::sleep(Duration::from_millis(250)), if state.remote.stopping.load(Ordering::Acquire) => {
                let _ = send_signal(socket, serde_json::json!({"type": "closed"})).await;
                break;
            }
        }
    }

    drop(stats_worker);
    drop(capture_worker);
    drop(input_sender);
    drop(data_channels);
    drop(peer_connection);
    Ok(())
}

fn save_received_file(app: &AppHandle, file_name: &str, contents: &[u8]) -> Result<String, String> {
    let (mut file, path) = create_received_file(app, file_name)?;
    if let Err(error) = file.write_all(contents) {
        drop(file);
        let _ = fs::remove_file(&path);
        return Err(format!("写入接收文件失败：{error}"));
    }
    if let Err(error) = file.sync_all() {
        drop(file);
        let _ = fs::remove_file(&path);
        return Err(format!("无法完成接收文件写入：{error}"));
    }
    Ok(path.to_string_lossy().into_owned())
}

fn create_received_file(app: &AppHandle, file_name: &str) -> Result<(File, PathBuf), String> {
    let mut file_name: String = file_name
        .chars()
        .map(|character| {
            if character.is_control()
                || matches!(
                    character,
                    '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|'
                )
            {
                '_'
            } else {
                character
            }
        })
        .collect();
    file_name = file_name.trim_matches([' ', '.']).to_string();
    if file_name.is_empty() {
        file_name = "received-file".to_string();
    }
    let mut directory = app
        .path()
        .download_dir()
        .map_err(|error| format!("无法定位下载目录：{error}"))?;
    directory.push("Lantern Received");
    fs::create_dir_all(&directory).map_err(|error| format!("无法创建文件接收目录：{error}"))?;

    let path = PathBuf::from(&file_name);
    let stem = path
        .file_stem()
        .and_then(|part| part.to_str())
        .unwrap_or("received-file");
    let extension = path.extension().and_then(|part| part.to_str());
    for suffix in 0..1000 {
        let candidate_name = match (suffix, extension) {
            (0, _) => file_name.clone(),
            (_, Some(extension)) => format!("{stem} ({suffix}).{extension}"),
            (_, None) => format!("{stem} ({suffix})"),
        };
        let candidate = directory.join(candidate_name);
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate)
        {
            Ok(file) => return Ok((file, candidate)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(format!("无法创建接收文件：{error}")),
        }
    }
    Err("接收目录中存在过多同名文件，无法保存".to_string())
}

async fn send_signal(
    socket: &mut WebSocket,
    message: serde_json::Value,
) -> Result<(), axum::Error> {
    socket.send(Message::Text(message.to_string().into())).await
}

fn is_input_message(message: &ClientMessage) -> bool {
    matches!(
        message,
        ClientMessage::MouseMove { .. }
            | ClientMessage::MouseButton { .. }
            | ClientMessage::MouseScroll { .. }
            | ClientMessage::Key { .. }
            | ClientMessage::ReleaseAll
    )
}

fn clear_pending(remote: &RemoteState, request_id: &str) {
    if let Ok(mut pending) = remote.pending_approval.lock() {
        if pending.as_ref().is_some_and(|(id, _)| id == request_id) {
            pending.take();
        }
        if let Ok(mut pending) = remote.pending_request.lock() {
            if pending
                .as_ref()
                .is_some_and(|request| request.request_id == request_id)
            {
                pending.take();
            }
        }
    }
}

fn local_lan_address() -> Option<IpAddr> {
    if let Ok(socket) = std::net::UdpSocket::bind("0.0.0.0:0") {
        if socket.connect("192.0.2.1:9").is_ok() {
            if let Ok(address) = socket.local_addr() {
                if is_lan_address(address.ip()) {
                    return Some(address.ip());
                }
            }
        }
    }
    None
}

fn is_lan_address(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(ip) => ip.is_private() || ip.is_loopback() || ip.is_link_local(),
        IpAddr::V6(ip) => ip.is_loopback() || ip.is_unique_local() || ip.is_unicast_link_local(),
    }
}

fn capture_dimensions() -> Result<(u32, u32), String> {
    let screen = primary_screen()?;
    Ok((screen.display_info.width, screen.display_info.height))
}

fn capture_bgra(max_width: u32) -> Result<(Vec<u8>, u32, u32), String> {
    let screen = primary_screen()?;
    let screenshot = screen
        .capture()
        .map_err(|error| format!("屏幕采集失败：{error}"))?;
    let width = screenshot.width();
    let height = screenshot.height();
    let pixels = image::RgbaImage::from_raw(width, height, screenshot.into_raw())
        .ok_or_else(|| "屏幕图像格式无效".to_string())?;

    let scale = (max_width as f64 / width as f64).min(1.0);
    let (frame_width, frame_height) = (
        (width as f64 * scale).round().max(1.0) as u32,
        (height as f64 * scale).round().max(1.0) as u32,
    );
    let resized = if frame_width == width && frame_height == height {
        pixels
    } else {
        image::imageops::resize(&pixels, frame_width, frame_height, FilterType::Triangle)
    };
    let mut output = resized.into_raw();
    for pixel in output.chunks_exact_mut(4) {
        pixel.swap(0, 2);
    }
    Ok((output, frame_width, frame_height))
}

fn primary_screen() -> Result<screenshots::Screen, String> {
    let screens = screenshots::Screen::all().map_err(|error| format!("无法读取屏幕：{error}"))?;
    screens
        .iter()
        .find(|screen| screen.display_info.is_primary)
        .or_else(|| screens.first())
        .copied()
        .ok_or_else(|| "没有找到可采集的屏幕".to_string())
}

#[derive(Default)]
struct InputState {
    pressed_keys: HashSet<Key>,
    pressed_buttons: HashSet<Button>,
    #[cfg(target_os = "windows")]
    pressed_scancodes: HashSet<u16>,
    scroll_remainder: f64,
}

fn apply_input(
    enigo: &mut Enigo,
    message: ClientMessage,
    width: u32,
    height: u32,
    state: &mut InputState,
) -> Result<(), String> {
    match message {
        ClientMessage::MouseMove { x, y } if x.is_finite() && y.is_finite() => {
            let x = (x.clamp(0.0, 1.0) * width.saturating_sub(1) as f64).round() as i32;
            let y = (y.clamp(0.0, 1.0) * height.saturating_sub(1) as f64).round() as i32;
            enigo
                .move_mouse(x, y, Coordinate::Abs)
                .map_err(|error| error.to_string())?;
        }
        ClientMessage::MouseButton { button, down } => {
            let button = match button.as_str() {
                "left" => Button::Left,
                "middle" => Button::Middle,
                "right" => Button::Right,
                _ => return Ok(()),
            };
            let direction = if down {
                state.pressed_buttons.insert(button);
                Direction::Press
            } else {
                state.pressed_buttons.remove(&button);
                Direction::Release
            };
            enigo
                .button(button, direction)
                .map_err(|error| error.to_string())?;
        }
        ClientMessage::Key { code, key, down } => {
            #[cfg(target_os = "windows")]
            if let Some(scancode) = physical_scancode(&code) {
                let direction = if down {
                    state.pressed_scancodes.insert(scancode);
                    Direction::Press
                } else {
                    state.pressed_scancodes.remove(&scancode);
                    Direction::Release
                };
                enigo
                    .raw(scancode, direction)
                    .map_err(|error| error.to_string())?;
                return Ok(());
            }
            if let Some(key) = translate_key(&code, &key) {
                let direction = if down {
                    state.pressed_keys.insert(key);
                    Direction::Press
                } else {
                    state.pressed_keys.remove(&key);
                    Direction::Release
                };
                enigo
                    .key(key, direction)
                    .map_err(|error| error.to_string())?;
            }
        }
        ClientMessage::MouseScroll { delta_y } if delta_y.is_finite() => {
            state.scroll_remainder += delta_y;
            let steps = (state.scroll_remainder / 100.0).trunc() as i32;
            if steps != 0 {
                state.scroll_remainder -= steps as f64 * 100.0;
                enigo
                    .scroll(steps, Axis::Vertical)
                    .map_err(|error| error.to_string())?;
            }
        }
        ClientMessage::Hello { .. } => {}
        ClientMessage::ClipboardText { .. }
        | ClientMessage::FileOffer { .. }
        | ClientMessage::FileComplete { .. }
        | ClientMessage::FileTransferComplete { .. }
        | ClientMessage::FileTransferDecision { .. } => {}
        ClientMessage::ReleaseAll => {
            for key in state.pressed_keys.drain() {
                enigo
                    .key(key, Direction::Release)
                    .map_err(|error| error.to_string())?;
            }
            for button in state.pressed_buttons.drain() {
                enigo
                    .button(button, Direction::Release)
                    .map_err(|error| error.to_string())?;
            }
            #[cfg(target_os = "windows")]
            for scancode in state.pressed_scancodes.drain() {
                enigo
                    .raw(scancode, Direction::Release)
                    .map_err(|error| error.to_string())?;
            }
        }
        ClientMessage::MouseMove { .. } => {}
        ClientMessage::MouseScroll { .. } => {}
        ClientMessage::Offer { .. } | ClientMessage::IceCandidate { .. } => {}
    }
    Ok(())
}

fn translate_key(code: &str, key: &str) -> Option<Key> {
    let special = match code {
        "Enter" => Some(Key::Return),
        "Escape" => Some(Key::Escape),
        "Backspace" => Some(Key::Backspace),
        "Tab" => Some(Key::Tab),
        "Space" => Some(Key::Space),
        "Delete" => Some(Key::Delete),
        "ArrowUp" => Some(Key::UpArrow),
        "ArrowDown" => Some(Key::DownArrow),
        "ArrowLeft" => Some(Key::LeftArrow),
        "ArrowRight" => Some(Key::RightArrow),
        "ShiftLeft" | "ShiftRight" => Some(Key::Shift),
        "ControlLeft" | "ControlRight" => Some(Key::Control),
        "AltLeft" | "AltRight" => Some(Key::Alt),
        "MetaLeft" | "MetaRight" => Some(Key::Meta),
        "Home" => Some(Key::Home),
        "End" => Some(Key::End),
        "PageUp" => Some(Key::PageUp),
        "PageDown" => Some(Key::PageDown),
        "CapsLock" => Some(Key::CapsLock),
        "F1" => Some(Key::F1),
        "F2" => Some(Key::F2),
        "F3" => Some(Key::F3),
        "F4" => Some(Key::F4),
        "F5" => Some(Key::F5),
        "F6" => Some(Key::F6),
        "F7" => Some(Key::F7),
        "F8" => Some(Key::F8),
        "F9" => Some(Key::F9),
        "F10" => Some(Key::F10),
        "F11" => Some(Key::F11),
        "F12" => Some(Key::F12),
        "F13" => Some(Key::F13),
        "F14" => Some(Key::F14),
        "F15" => Some(Key::F15),
        "F16" => Some(Key::F16),
        "F17" => Some(Key::F17),
        "F18" => Some(Key::F18),
        "F19" => Some(Key::F19),
        "F20" => Some(Key::F20),
        _ => None,
    };
    special.or_else(|| {
        let mut characters = key.chars();
        let character = characters.next()?;
        (characters.next().is_none() && !character.is_control()).then_some(Key::Unicode(character))
    })
}

#[cfg(any(target_os = "windows", test))]
fn physical_scancode(code: &str) -> Option<u16> {
    Some(match code {
        "KeyA" => 0x1e,
        "KeyB" => 0x30,
        "KeyC" => 0x2e,
        "KeyD" => 0x20,
        "KeyE" => 0x12,
        "KeyF" => 0x21,
        "KeyG" => 0x22,
        "KeyH" => 0x23,
        "KeyI" => 0x17,
        "KeyJ" => 0x24,
        "KeyK" => 0x25,
        "KeyL" => 0x26,
        "KeyM" => 0x32,
        "KeyN" => 0x31,
        "KeyO" => 0x18,
        "KeyP" => 0x19,
        "KeyQ" => 0x10,
        "KeyR" => 0x13,
        "KeyS" => 0x1f,
        "KeyT" => 0x14,
        "KeyU" => 0x16,
        "KeyV" => 0x2f,
        "KeyW" => 0x11,
        "KeyX" => 0x2d,
        "KeyY" => 0x15,
        "KeyZ" => 0x2c,
        "Digit1" => 0x02,
        "Digit2" => 0x03,
        "Digit3" => 0x04,
        "Digit4" => 0x05,
        "Digit5" => 0x06,
        "Digit6" => 0x07,
        "Digit7" => 0x08,
        "Digit8" => 0x09,
        "Digit9" => 0x0a,
        "Digit0" => 0x0b,
        "Minus" => 0x0c,
        "Equal" => 0x0d,
        "BracketLeft" => 0x1a,
        "BracketRight" => 0x1b,
        "Backslash" => 0x2b,
        "IntlBackslash" => 0x56,
        "Semicolon" => 0x27,
        "Quote" => 0x28,
        "Backquote" => 0x29,
        "Comma" => 0x33,
        "Period" => 0x34,
        "Slash" => 0x35,
        "ShiftLeft" => 0x2a,
        "ShiftRight" => 0x36,
        "ControlLeft" | "MetaLeft" => 0x1d,
        "ControlRight" | "MetaRight" => 0xe01d,
        "AltLeft" => 0x38,
        "AltRight" => 0xe038,
        "Enter" => 0x1c,
        "NumpadEnter" => 0xe01c,
        "Escape" => 0x01,
        "Backspace" => 0x0e,
        "Tab" => 0x0f,
        "Space" => 0x39,
        "CapsLock" => 0x3a,
        "F1" => 0x3b,
        "F2" => 0x3c,
        "F3" => 0x3d,
        "F4" => 0x3e,
        "F5" => 0x3f,
        "F6" => 0x40,
        "F7" => 0x41,
        "F8" => 0x42,
        "F9" => 0x43,
        "F10" => 0x44,
        "F11" => 0x57,
        "F12" => 0x58,
        "PrintScreen" => 0xe037,
        "ScrollLock" => 0x46,
        "Pause" => 0x45,
        "Insert" => 0xe052,
        "Home" => 0xe047,
        "PageUp" => 0xe049,
        "Delete" => 0xe053,
        "End" => 0xe04f,
        "PageDown" => 0xe051,
        "ArrowUp" => 0xe048,
        "ArrowLeft" => 0xe04b,
        "ArrowDown" => 0xe050,
        "ArrowRight" => 0xe04d,
        "NumLock" => 0xe045,
        "NumpadDivide" => 0xe035,
        "NumpadMultiply" => 0x37,
        "NumpadSubtract" => 0x4a,
        "NumpadAdd" => 0x4e,
        "NumpadDecimal" => 0x53,
        "Numpad0" => 0x52,
        "Numpad1" => 0x4f,
        "Numpad2" => 0x50,
        "Numpad3" => 0x51,
        "Numpad4" => 0x4b,
        "Numpad5" => 0x4c,
        "Numpad6" => 0x4d,
        "Numpad7" => 0x47,
        "Numpad8" => 0x48,
        "Numpad9" => 0x49,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn video_quality_modes_have_distinct_resolution_and_rate_targets() {
        let quality = VideoQuality::Quality.settings();
        let balanced = VideoQuality::Balanced.settings();
        let smooth = VideoQuality::Smooth.settings();

        assert!(quality.max_width > balanced.max_width);
        assert!(smooth.max_width < balanced.max_width);
        assert!(smooth.frame_interval < balanced.frame_interval);
        assert!(quality.max_bitrate > balanced.max_bitrate);
    }

    #[test]
    fn hello_handshake_defaults_to_balanced_for_older_controllers() {
        let hello =
            serde_json::from_str::<ClientMessage>(r#"{"type":"hello","device_name":"Lantern"}"#)
                .unwrap();
        assert!(matches!(
            hello,
            ClientMessage::Hello {
                quality: VideoQuality::Balanced,
                ..
            }
        ));
    }

    #[test]
    fn clipboard_and_file_messages_decode_with_expected_fields() {
        assert!(matches!(
            serde_json::from_str::<ClientMessage>(
                r#"{"type":"clipboard_text","text":"shared text"}"#
            ),
            Ok(ClientMessage::ClipboardText { text }) if text == "shared text"
        ));
        assert!(matches!(
            serde_json::from_str::<ClientMessage>(
                r#"{"type":"hello","device_name":"Lantern","clipboard_enabled":true}"#
            ),
            Ok(ClientMessage::Hello {
                clipboard_enabled: true,
                ..
            })
        ));
        assert!(matches!(
            serde_json::from_str::<ClientMessage>(
                r#"{"type":"file_offer","transfer_id":"transfer-1","file_name":"notes.txt","size":12}"#
            ),
            Ok(ClientMessage::FileOffer {
                transfer_id,
                file_name,
                size: 12
            }) if transfer_id == "transfer-1" && file_name == "notes.txt"
        ));
        assert!(matches!(
            serde_json::from_str::<ClientMessage>(
                r#"{"type":"file_complete","transfer_id":"transfer-1"}"#
            ),
            Ok(ClientMessage::FileComplete { transfer_id }) if transfer_id == "transfer-1"
        ));
        assert!(matches!(
            serde_json::from_str::<ClientMessage>(
                r#"{"type":"file_transfer_decision","transfer_id":"transfer-1","accepted":true}"#
            ),
            Ok(ClientMessage::FileTransferDecision { transfer_id, accepted: true })
                if transfer_id == "transfer-1"
        ));
        assert!(matches!(
            serde_json::from_str::<ClientMessage>(
                r#"{"type":"file_transfer_complete","transfer_id":"transfer-1"}"#
            ),
            Ok(ClientMessage::FileTransferComplete { transfer_id }) if transfer_id == "transfer-1"
        ));
    }

    #[test]
    fn only_local_network_peers_are_allowed() {
        assert!(is_lan_address("192.168.1.24".parse().unwrap()));
        assert!(is_lan_address("10.0.0.7".parse().unwrap()));
        assert!(is_lan_address("172.20.0.8".parse().unwrap()));
        assert!(is_lan_address("fe80::1".parse().unwrap()));
        assert!(!is_lan_address("8.8.8.8".parse().unwrap()));
        assert!(!is_lan_address("2001:4860:4860::8888".parse().unwrap()));
    }

    #[test]
    fn input_messages_require_supported_shapes() {
        assert!(matches!(
            serde_json::from_str::<ClientMessage>(r#"{"type":"mouse_move","x":0.5,"y":0.25}"#),
            Ok(ClientMessage::MouseMove { x: 0.5, y: 0.25 })
        ));
        assert!(
            serde_json::from_str::<ClientMessage>(r#"{"type":"mouse_move","x":"left","y":0}"#)
                .is_err()
        );
        assert!(
            serde_json::from_str::<ClientMessage>(
                r#"{"type":"execute_command","command":"anything"}"#
            )
            .is_err()
        );
    }

    #[test]
    fn webrtc_signaling_messages_accept_browser_payloads() {
        assert!(matches!(
            serde_json::from_str::<ClientMessage>(r#"{"type":"offer","sdp":"v=0"}"#),
            Ok(ClientMessage::Offer { sdp }) if sdp == "v=0"
        ));
        assert!(matches!(
            serde_json::from_str::<ClientMessage>(
                r#"{"type":"ice_candidate","candidate":"candidate:1","sdpMid":"0","sdpMLineIndex":0}"#
            ),
            Ok(ClientMessage::IceCandidate { candidate, sdp_mid: Some(mid), sdp_mline_index: Some(0) })
                if candidate == "candidate:1" && mid == "0"
        ));
        assert!(!is_input_message(
            &serde_json::from_str::<ClientMessage>(r#"{"type":"offer","sdp":"v=0"}"#).unwrap()
        ));
    }

    #[test]
    fn keyboard_translation_limits_input_to_keys() {
        assert_eq!(translate_key("Enter", "Enter"), Some(Key::Return));
        #[cfg(not(target_os = "windows"))]
        assert_eq!(translate_key("KeyA", "a"), Some(Key::Unicode('a')));
        assert_eq!(translate_key("F5", "F5"), Some(Key::F5));
        assert_eq!(translate_key("UnmappedKey", "ab"), None);
    }

    #[test]
    fn physical_scancodes_cover_shortcuts_and_shifted_punctuation() {
        assert_eq!(physical_scancode("MetaLeft"), Some(0x1d));
        assert_eq!(physical_scancode("ControlRight"), Some(0xe01d));
        assert_eq!(physical_scancode("KeyC"), Some(0x2e));
        assert_eq!(physical_scancode("Digit5"), Some(0x06));
        assert_eq!(physical_scancode("ShiftLeft"), Some(0x2a));
        assert_eq!(physical_scancode("Semicolon"), Some(0x27));
        assert_eq!(physical_scancode("UnmappedKey"), None);
    }
}
