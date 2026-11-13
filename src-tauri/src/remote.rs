use std::{
    collections::{HashMap, HashSet},
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    net::{IpAddr, Ipv4Addr, SocketAddr, ToSocketAddrs},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::{Duration, SystemTime},
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
use if_addrs::{IfAddr, get_if_addrs};
use image::imageops::FilterType;
use reactor_webrtc::{
    AudioFrame, AudioTrack, AudioTrackOptions, AudioTrackSource, IceCandidate, MediaKind,
    PeerConnection, PeerConnectionFactory, PeerConnectionObserver, PeerConnectionState,
    RtcConfiguration, SdpType, SessionDescription, StreamKind, TransceiverDirection, VideoCodec,
    VideoFrame,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tauri::Manager;
use tauri::{AppHandle, Emitter};
use tauri_plugin_dialog::DialogExt;
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::timeout,
};
use uuid::Uuid;

const DEFAULT_PORT: u16 = 4200;
const DISCOVERY_PORT: u16 = 42011;
const APPROVAL_TIMEOUT: Duration = Duration::from_secs(60);
const SIGNALING_TIMEOUT: Duration = Duration::from_secs(20);
const MAX_FILE_TRANSFER_SIZE: u64 = 100 * 1024 * 1024 * 1024;
const MAX_CLIPBOARD_TEXT_SIZE: usize = 48 * 1024;
const MAX_FILE_CHUNK_SIZE: usize = 1024 * 1024;
// WebSocket 单条消息上限：需容纳 1 MiB 文件数据块。
const MAX_SIGNAL_MESSAGE_SIZE: usize = 2 * 1024 * 1024;
// 被控端发送文件时允许未被控制端确认落盘的最大字节数，用于限制控制端内存占用。
const FILE_SEND_WINDOW: u64 = 16 * 1024 * 1024;
const FILE_PROGRESS_EVENT_STEP: u64 = 4 * 1024 * 1024;
const FILE_PROGRESS_LOG_STEP: u64 = 64 * 1024 * 1024;
const FILE_HASH_PROGRESS_STEP: u64 = 64 * 1024 * 1024;
const MAX_PREPARED_OUTGOING_FILES: usize = 16;
// 每接收这么多数据做一次 fsync 并记录「已确认落盘」位置；崩溃后只回退到最近的检查点。
const PARTIAL_SYNC_CHECKPOINT: u64 = 64 * 1024 * 1024;
// 旧版续传记录没有检查点信息时，续传前保守回退的长度。
const LEGACY_RESUME_ROLLBACK: u64 = 8 * 1024 * 1024;
// 接受文件前额外预留的磁盘空间。
const DISK_SPACE_MARGIN: u64 = 256 * 1024 * 1024;
// 分段哈希的段大小：最终校验失败时，只回退到第一个损坏段的开头续传。
const FILE_SEGMENT_SIZE: u64 = 64 * 1024 * 1024;
const MAX_FILE_HASH_CACHE_ENTRIES: usize = 64;
const MAX_DIAGNOSTIC_LOG_SIZE: u64 = 512 * 1024;
const PARTIAL_TRANSFER_TTL: Duration = Duration::from_secs(30 * 24 * 60 * 60);
static DIAGNOSTIC_LOG_LOCK: Mutex<()> = Mutex::new(());

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticEvent {
    DiscoveryStarted,
    DiscoveryCompleted,
    DiscoveryFailed,
    ConnectionStarted,
    ConnectionRequestReceived,
    ConnectionRequestApproved,
    ConnectionRequestRejected,
    ConnectionApproved,
    ConnectionEstablished,
    ConnectionFailed,
    ConnectionInterrupted,
    ConnectionClosed,
    SignalingOfferReceived,
    WebRtcAnswerCreated,
    IceCandidateSent,
    IceCandidateReceived,
    AgentConnectionApproved,
    AgentConnectionEstablished,
    AgentConnectionFailed,
    AgentConnectionInterrupted,
    AgentConnectionClosed,
    AgentStarted,
    AgentStopped,
    AgentStartFailed,
    AgentStopFailed,
    AudioNegotiationSendOnly,
    AudioNegotiationMissing,
    AudioNegotiationNotRequested,
    AudioTrackCreated,
    AudioOutboundRtpMissing,
    AudioInboundRtpMissing,
    AudioInboundStatsReadFailed,
    FileTransferStarted,
    FileTransferOfferReceived,
    FileTransferAccepted,
    FileTransferRejected,
    FileTransferPaused,
    FileTransferResumed,
    FileTransferCancelled,
    FileTransferProgress,
    FileTransferInterrupted,
    FileTransferCompleted,
    FileTransferFailed,
    AudioCaptureStarted,
    AudioCaptureSignalDetected,
    AudioCaptureSignalMissing,
    AudioFramesSubmitted,
    AudioOutboundRtpStats,
    AudioInboundRtpStats,
    AudioRemoteTrackReceived,
    AudioRemoteTrackUnmuted,
    AudioPlaybackStarted,
    AudioPlaybackBlocked,
    AudioPlaybackFailed,
    AudioCaptureFailed,
    VideoCaptureFailed,
    ClipboardSyncStarted,
    ClipboardTextSent,
    ClipboardTextReceived,
    ClipboardSyncFailed,
    VideoTrackCreated,
    VideoFramesSubmitted,
    WebRtcStateConnected,
    WebRtcStateDisconnected,
    WebRtcStateFailed,
    WebRtcStatsReadFailed,
    InputMessagesProcessed,
    QualityReduced,
    QualityRestored,
}

fn system_audio_to_pcm(samples: &[f32]) -> Vec<i16> {
    samples
        .iter()
        .map(|sample| (sample.clamp(-1.0, 1.0) * i16::MAX as f32).round() as i16)
        .collect()
}

impl DiagnosticEvent {
    fn as_str(self) -> &'static str {
        match self {
            Self::DiscoveryStarted => "discovery_started",
            Self::DiscoveryCompleted => "discovery_completed",
            Self::DiscoveryFailed => "discovery_failed",
            Self::ConnectionStarted => "connection_started",
            Self::ConnectionRequestReceived => "connection_request_received",
            Self::ConnectionRequestApproved => "connection_request_approved",
            Self::ConnectionRequestRejected => "connection_request_rejected",
            Self::ConnectionApproved => "connection_approved",
            Self::ConnectionEstablished => "connection_established",
            Self::ConnectionFailed => "connection_failed",
            Self::ConnectionInterrupted => "connection_interrupted",
            Self::ConnectionClosed => "connection_closed",
            Self::SignalingOfferReceived => "signaling_offer_received",
            Self::WebRtcAnswerCreated => "webrtc_answer_created",
            Self::IceCandidateSent => "ice_candidate_sent",
            Self::IceCandidateReceived => "ice_candidate_received",
            Self::AgentConnectionApproved => "agent_connection_approved",
            Self::AgentConnectionEstablished => "agent_connection_established",
            Self::AgentConnectionFailed => "agent_connection_failed",
            Self::AgentConnectionInterrupted => "agent_connection_interrupted",
            Self::AgentConnectionClosed => "agent_connection_closed",
            Self::AgentStarted => "agent_started",
            Self::AgentStopped => "agent_stopped",
            Self::AgentStartFailed => "agent_start_failed",
            Self::AgentStopFailed => "agent_stop_failed",
            Self::AudioNegotiationSendOnly => "audio_negotiation_sendonly",
            Self::AudioNegotiationMissing => "audio_negotiation_missing",
            Self::AudioNegotiationNotRequested => "audio_negotiation_not_requested",
            Self::AudioTrackCreated => "audio_track_created",
            Self::AudioOutboundRtpMissing => "audio_outbound_rtp_missing",
            Self::AudioInboundRtpMissing => "audio_inbound_rtp_missing",
            Self::AudioInboundStatsReadFailed => "audio_inbound_stats_read_failed",
            Self::FileTransferStarted => "file_transfer_started",
            Self::FileTransferOfferReceived => "file_transfer_offer_received",
            Self::FileTransferAccepted => "file_transfer_accepted",
            Self::FileTransferRejected => "file_transfer_rejected",
            Self::FileTransferPaused => "file_transfer_paused",
            Self::FileTransferResumed => "file_transfer_resumed",
            Self::FileTransferCancelled => "file_transfer_cancelled",
            Self::FileTransferProgress => "file_transfer_progress",
            Self::FileTransferInterrupted => "file_transfer_interrupted",
            Self::FileTransferCompleted => "file_transfer_completed",
            Self::FileTransferFailed => "file_transfer_failed",
            Self::AudioCaptureStarted => "audio_capture_started",
            Self::AudioCaptureSignalDetected => "audio_capture_signal_detected",
            Self::AudioCaptureSignalMissing => "audio_capture_signal_missing",
            Self::AudioFramesSubmitted => "audio_frames_submitted",
            Self::AudioOutboundRtpStats => "audio_outbound_rtp_stats",
            Self::AudioInboundRtpStats => "audio_inbound_rtp_stats",
            Self::AudioRemoteTrackReceived => "audio_remote_track_received",
            Self::AudioRemoteTrackUnmuted => "audio_remote_track_unmuted",
            Self::AudioPlaybackStarted => "audio_playback_started",
            Self::AudioPlaybackBlocked => "audio_playback_blocked",
            Self::AudioPlaybackFailed => "audio_playback_failed",
            Self::AudioCaptureFailed => "audio_capture_failed",
            Self::VideoCaptureFailed => "video_capture_failed",
            Self::ClipboardSyncStarted => "clipboard_sync_started",
            Self::ClipboardTextSent => "clipboard_text_sent",
            Self::ClipboardTextReceived => "clipboard_text_received",
            Self::ClipboardSyncFailed => "clipboard_sync_failed",
            Self::VideoTrackCreated => "video_track_created",
            Self::VideoFramesSubmitted => "video_frames_submitted",
            Self::WebRtcStateConnected => "webrtc_state_connected",
            Self::WebRtcStateDisconnected => "webrtc_state_disconnected",
            Self::WebRtcStateFailed => "webrtc_state_failed",
            Self::WebRtcStatsReadFailed => "webrtc_stats_read_failed",
            Self::InputMessagesProcessed => "input_messages_processed",
            Self::QualityReduced => "quality_reduced",
            Self::QualityRestored => "quality_restored",
        }
    }
}

fn diagnostic_log_path(app: &AppHandle) -> Result<PathBuf, String> {
    let mut path = app
        .path()
        .app_log_dir()
        .map_err(|error| format!("无法定位诊断日志目录：{error}"))?;
    path.push("lantern.log");
    Ok(path)
}

fn append_diagnostic_log(app: &AppHandle, event: DiagnosticEvent) -> Result<(), String> {
    let _guard = DIAGNOSTIC_LOG_LOCK
        .lock()
        .map_err(|_| "诊断日志状态不可用。".to_string())?;
    let path = diagnostic_log_path(app)?;
    append_diagnostic_log_file(&path, event)
}

fn append_diagnostic_log_values(
    app: &AppHandle,
    event: DiagnosticEvent,
    value: Option<u64>,
    value2: Option<u64>,
) -> Result<(), String> {
    let _guard = DIAGNOSTIC_LOG_LOCK
        .lock()
        .map_err(|_| "诊断日志状态不可用。".to_string())?;
    let path = diagnostic_log_path(app)?;
    append_diagnostic_log_file_values(&path, event, value, value2)
}

fn append_diagnostic_log_file(path: &Path, event: DiagnosticEvent) -> Result<(), String> {
    append_diagnostic_log_file_values(path, event, None, None)
}

fn append_diagnostic_log_file_values(
    path: &Path,
    event: DiagnosticEvent,
    value: Option<u64>,
    value2: Option<u64>,
) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| "无法定位诊断日志目录。".to_string())?;
    fs::create_dir_all(parent).map_err(|error| format!("无法创建诊断日志目录：{error}"))?;
    if fs::metadata(&path).is_ok_and(|metadata| metadata.len() >= MAX_DIAGNOSTIC_LOG_SIZE) {
        let mut previous = path.to_path_buf();
        previous.set_extension("log.1");
        if previous.exists() {
            fs::remove_file(&previous).map_err(|error| format!("无法轮换旧诊断日志：{error}"))?;
        }
        fs::rename(&path, previous).map_err(|error| format!("无法轮换诊断日志：{error}"))?;
    }
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| format!("无法读取系统时间：{error}"))?
        .as_millis();
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|error| format!("无法打开诊断日志：{error}"))?;
    write!(file, "{timestamp}\t{}", event.as_str())
        .map_err(|error| format!("无法写入诊断日志：{error}"))?;
    if let Some(value) = value {
        write!(file, "\tvalue={value}").map_err(|error| format!("无法写入诊断日志：{error}"))?;
    }
    if let Some(value2) = value2 {
        write!(file, "\tvalue2={value2}").map_err(|error| format!("无法写入诊断日志：{error}"))?;
    }
    writeln!(file).map_err(|error| format!("无法写入诊断日志：{error}"))
}

#[tauri::command]
pub fn log_diagnostic_event(
    app: AppHandle,
    event: DiagnosticEvent,
    value: Option<u64>,
    value2: Option<u64>,
) -> Result<(), String> {
    append_diagnostic_log_values(&app, event, value, value2)
}

#[tauri::command]
pub fn get_diagnostic_log(app: AppHandle) -> Result<String, String> {
    let _guard = DIAGNOSTIC_LOG_LOCK
        .lock()
        .map_err(|_| "诊断日志状态不可用。".to_string())?;
    read_diagnostic_log_file(&diagnostic_log_path(&app)?)
}

fn read_diagnostic_log_file(path: &Path) -> Result<String, String> {
    let mut previous_path = path.to_path_buf();
    previous_path.set_extension("log.1");
    let previous = match fs::read_to_string(previous_path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(error) => return Err(format!("无法读取上一份诊断日志：{error}")),
    };
    match fs::read_to_string(path) {
        Ok(contents) => Ok(format!("{previous}{contents}")),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(previous),
        Err(error) => Err(format!("无法读取诊断日志：{error}")),
    }
}

fn is_valid_file_offer(transfer_id: &str, file_name: &str, size: u64, sha256: &str) -> bool {
    is_valid_transfer_id(transfer_id)
        && !file_name.is_empty()
        && file_name.len() <= 255
        && !file_name
            .chars()
            .any(|character| character.is_control() || matches!(character, '/' | '\\'))
        && size > 0
        && size <= MAX_FILE_TRANSFER_SIZE
        && sha256.len() == 64
        && sha256
            .chars()
            .all(|character| character.is_ascii_hexdigit())
}

/// 分段哈希列表允许为空（兼容旧版发送端，此时校验失败只能整体重传）；
/// 非空时段数必须与文件大小一致，每段都是 64 位十六进制 SHA-256。
fn is_valid_segment_hashes(size: u64, segment_hashes: &[String]) -> bool {
    segment_hashes.is_empty()
        || (segment_hashes.len() as u64 == size.div_ceil(FILE_SEGMENT_SIZE)
            && segment_hashes.iter().all(|hash| {
                hash.len() == 64 && hash.chars().all(|character| character.is_ascii_hexdigit())
            }))
}

fn is_valid_transfer_id(transfer_id: &str) -> bool {
    !transfer_id.is_empty()
        && transfer_id.len() <= 64
        && transfer_id
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
}

#[derive(Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum VideoQuality {
    Quality,
    #[default]
    Balanced,
    Smooth,
}

#[derive(Clone, Copy, PartialEq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum AdaptiveQuality {
    Normal,
    Constrained,
}

fn bitrate_for_network(settings: VideoSettings, quality: AdaptiveQuality) -> (i32, i32) {
    match quality {
        AdaptiveQuality::Normal => (settings.min_bitrate, settings.max_bitrate),
        AdaptiveQuality::Constrained => (
            (settings.min_bitrate / 2).max(1_000_000),
            (settings.max_bitrate * 65 / 100).max(2_000_000),
        ),
    }
}

#[tauri::command]
pub fn set_clipboard_sync_enabled(remote: tauri::State<'_, Arc<RemoteState>>, enabled: bool) {
    remote
        .clipboard_sync_enabled
        .store(enabled, Ordering::Release);
}

#[tauri::command]
pub fn set_system_audio_sync_enabled(remote: tauri::State<'_, Arc<RemoteState>>, enabled: bool) {
    remote
        .system_audio_sync_enabled
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
                    let _ = append_diagnostic_log(&app, DiagnosticEvent::ClipboardSyncFailed);
                    let _ = app.emit("clipboard-status", format!("无法监视系统剪贴板：{error}"));
                    return;
                }
            };
            let mut watching = false;
            let mut read_error_reported = false;
            let mut consecutive_read_failures = 0_u32;
            let mut oversized_text_reported = false;
            while !stopping_for_thread.load(Ordering::Acquire) {
                thread::sleep(Duration::from_millis(400));
                if !remote.clipboard_sync_enabled.load(Ordering::Acquire) {
                    watching = false;
                    continue;
                }
                let text = match read_clipboard_text(&mut clipboard) {
                    Ok(Some(text)) => {
                        read_error_reported = false;
                        consecutive_read_failures = 0;
                        text
                    }
                    // 剪贴板里不是文本（图片、文件或为空），不同步也不算错误。
                    Ok(None) => {
                        consecutive_read_failures = 0;
                        continue;
                    }
                    Err(_) => {
                        // Windows 剪贴板常被其他程序短暂占用，连续失败约 4 秒才提示。
                        consecutive_read_failures += 1;
                        if consecutive_read_failures >= 10 && !read_error_reported {
                            let _ =
                                append_diagnostic_log(&app, DiagnosticEvent::ClipboardSyncFailed);
                            let _ = app.emit(
                                "clipboard-status",
                                "无法读取系统剪贴板，请检查剪贴板访问权限。",
                            );
                            read_error_reported = true;
                        }
                        continue;
                    }
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
                        let _ = append_diagnostic_log(&app, DiagnosticEvent::ClipboardSyncFailed);
                        let _ = app.emit("clipboard-status", "剪贴板同步状态不可用");
                        return;
                    }
                };
                if !changed {
                    continue;
                }
                if text.len() > MAX_CLIPBOARD_TEXT_SIZE {
                    if !oversized_text_reported {
                        let _ = append_diagnostic_log(&app, DiagnosticEvent::ClipboardSyncFailed);
                        let _ = app.emit("clipboard-status", "剪贴板文本超过 48 KiB，未同步。");
                        oversized_text_reported = true;
                    }
                    continue;
                }
                oversized_text_reported = false;
                if sender.send(text).is_err() {
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

/// 读取剪贴板文本。剪贴板中没有文本（为空、是图片或文件）时返回 None；
/// 剪贴板被其他程序短暂占用时（Windows 上很常见）稍等后重试。
fn read_clipboard_text(clipboard: &mut Clipboard) -> Result<Option<String>, arboard::Error> {
    let mut attempts = 0;
    loop {
        match clipboard.get_text() {
            Ok(text) => return Ok(Some(text)),
            Err(arboard::Error::ContentNotAvailable) => return Ok(None),
            Err(arboard::Error::ClipboardOccupied) if attempts < 5 => {
                attempts += 1;
                thread::sleep(Duration::from_millis(30));
            }
            Err(error) => return Err(error),
        }
    }
}

#[tauri::command(async)]
pub fn get_system_clipboard_text() -> Result<Option<String>, String> {
    let mut clipboard = Clipboard::new().map_err(|error| format!("无法访问系统剪贴板：{error}"))?;
    read_clipboard_text(&mut clipboard).map_err(|error| format!("无法读取系统剪贴板：{error}"))
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AvailableDisplay {
    id: u32,
    name: String,
    width: u32,
    height: u32,
    primary: bool,
}

#[tauri::command]
pub fn get_available_displays() -> Result<Vec<AvailableDisplay>, String> {
    let monitors = capture_monitors().map_err(|error| format!("无法读取显示器列表：{error}"))?;
    Ok(monitors
        .iter()
        .enumerate()
        .map(|(index, monitor)| AvailableDisplay {
            id: monitor.id,
            name: if monitor.primary {
                format!("显示器 {}（主屏幕）", index + 1)
            } else {
                format!("显示器 {}", index + 1)
            },
            width: monitor.width,
            height: monitor.height,
            primary: monitor.primary,
        })
        .collect())
}

#[tauri::command]
pub fn get_capture_display(
    remote: tauri::State<'_, Arc<RemoteState>>,
) -> Result<Option<u32>, String> {
    remote
        .capture_display_id
        .lock()
        .map(|display_id| *display_id)
        .map_err(|_| "显示器选择状态不可用。".to_string())
}

#[tauri::command]
pub fn set_capture_display(
    remote: tauri::State<'_, Arc<RemoteState>>,
    display_id: Option<u32>,
) -> Result<(), String> {
    if let Some(display_id) = display_id
        && !capture_monitors()
            .map_err(|error| format!("无法验证显示器：{error}"))?
            .iter()
            .any(|monitor| monitor.id == display_id)
    {
        return Err("所选显示器当前不可用，请刷新显示器列表。".to_string());
    }
    *remote
        .capture_display_id
        .lock()
        .map_err(|_| "显示器选择状态不可用。".to_string())? = display_id;
    Ok(())
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

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PreparedOutgoingFile {
    source_id: String,
    file_name: String,
    size: u64,
    sha256: String,
    segment_hashes: Vec<String>,
}

/// 选择要发送的文件：在 Rust 侧弹出系统文件选择框、流式计算 SHA-256，
/// 并登记为可发送的文件源。前端只拿到 sourceId 和文件元信息，不再读取文件内容。
#[tauri::command]
pub async fn prepare_outgoing_file(
    app: AppHandle,
    remote: tauri::State<'_, Arc<RemoteState>>,
    on_progress: tauri::ipc::Channel<serde_json::Value>,
) -> Result<Option<PreparedOutgoingFile>, String> {
    let (sender, receiver) = oneshot::channel();
    app.dialog()
        .file()
        .set_title("选择要发送的文件")
        .pick_file(move |path| {
            let _ = sender.send(path);
        });
    let Some(path) = receiver
        .await
        .map_err(|_| "文件选择窗口已关闭。".to_string())?
    else {
        return Ok(None);
    };
    let path = path
        .into_path()
        .map_err(|error| format!("无法读取所选文件路径：{error}"))?;
    let state = Arc::clone(remote.inner());
    let source = tokio::task::spawn_blocking(move || {
        prepare_outgoing_file_source(&state, path, &on_progress)
    })
    .await
    .map_err(|error| format!("文件校验任务失败：{error}"))??;
    let source_id = Uuid::new_v4().to_string();
    let prepared = PreparedOutgoingFile {
        source_id: source_id.clone(),
        file_name: source.file_name.clone(),
        size: source.size,
        sha256: source.sha256.clone(),
        segment_hashes: source.segment_hashes.clone(),
    };
    let mut files = remote
        .outgoing_files
        .lock()
        .map_err(|_| "文件发送状态不可用".to_string())?;
    if files.len() >= MAX_PREPARED_OUTGOING_FILES {
        files.remove(0);
    }
    files.push((source_id, Arc::new(source)));
    Ok(Some(prepared))
}

/// 控制端发送文件时按偏移读取一个数据块，以原始二进制返回给前端（不经过 JSON 数组序列化）。
#[tauri::command]
pub async fn read_outgoing_file_chunk(
    remote: tauri::State<'_, Arc<RemoteState>>,
    source_id: String,
    offset: u64,
) -> Result<tauri::ipc::Response, String> {
    let source = prepared_outgoing_file(remote.inner(), &source_id)?;
    let bytes = tokio::task::spawn_blocking(move || {
        let mut file = open_outgoing_file_source(&source, offset)?;
        read_outgoing_chunk(&mut file, &source, offset)
    })
    .await
    .map_err(|error| format!("读取文件任务失败：{error}"))??;
    Ok(tauri::ipc::Response::new(bytes))
}

#[tauri::command]
pub fn release_outgoing_file(
    remote: tauri::State<'_, Arc<RemoteState>>,
    source_id: String,
) -> Result<(), String> {
    remote
        .outgoing_files
        .lock()
        .map_err(|_| "文件发送状态不可用".to_string())?
        .retain(|(id, _)| id.as_str() != source_id.as_str());
    Ok(())
}

#[tauri::command]
pub fn begin_agent_file_transfer(
    remote: tauri::State<'_, Arc<RemoteState>>,
    transfer_id: String,
    source_id: String,
) -> Result<(), String> {
    // 被控端发送时文件由 Rust 传输任务持有，这里直接从登记表中取走，传完自动释放。
    let source = take_prepared_outgoing_file(remote.inner(), &source_id)?;
    if !is_valid_file_offer(&transfer_id, &source.file_name, source.size, &source.sha256) {
        return Err("文件名或大小无效，文件不能超过 100 GiB。".to_string());
    }
    let sender = remote
        .file_sender
        .lock()
        .map_err(|_| "文件传输状态不可用".to_string())?
        .clone()
        .ok_or_else(|| "当前没有可用的控制连接。".to_string())?;
    sender
        .send(AgentFileCommand::Offer {
            transfer_id,
            source,
        })
        .map_err(|_| "文件传输通道已关闭，请重新连接后重试。".to_string())?;
    Ok(())
}

#[tauri::command]
pub fn control_agent_file_transfer(
    remote: tauri::State<'_, Arc<RemoteState>>,
    transfer_id: String,
    action: String,
) -> Result<(), String> {
    if transfer_id.is_empty()
        || transfer_id.len() > 64
        || !matches!(action.as_str(), "pause" | "resume" | "cancel")
    {
        return Err("文件传输控制请求无效。".to_string());
    }
    let sender = remote
        .file_sender
        .lock()
        .map_err(|_| "文件传输状态不可用".to_string())?
        .clone()
        .ok_or_else(|| "当前没有可用的控制连接。".to_string())?;
    sender
        .send(AgentFileCommand::Control {
            transfer_id,
            action,
        })
        .map_err(|_| "文件传输通道已关闭，请重新连接后重试。".to_string())
}

#[tauri::command]
pub fn begin_controller_file_receive(
    app: AppHandle,
    remote: tauri::State<'_, Arc<RemoteState>>,
    transfer_id: String,
    file_name: String,
    size: u64,
    sha256: String,
    segment_hashes: Vec<String>,
) -> Result<u64, String> {
    if !is_valid_file_offer(&transfer_id, &file_name, size, &sha256)
        || !is_valid_segment_hashes(size, &segment_hashes)
    {
        return Err("文件接收请求无效或超过 100 GiB 限制".to_string());
    }
    let mut pending = remote
        .controller_file_receiver
        .lock()
        .map_err(|_| "文件接收状态不可用".to_string())?;
    if pending.is_some() {
        return Err("已有文件正在接收。".to_string());
    }
    let receiver =
        open_partial_file_receiver(&app, transfer_id, file_name, size, sha256, segment_hashes)?;
    let offset = receiver.received_size;
    *pending = Some(receiver);
    Ok(offset)
}

/// 控制端接收文件数据块。前端以原始二进制请求体调用（Uint8Array），
/// 传输标识放在 `x-transfer-id` 请求头里；返回已落盘的总字节数，用于向被控端确认进度。
#[tauri::command(async)]
pub fn append_controller_file_chunk(
    remote: tauri::State<'_, Arc<RemoteState>>,
    request: tauri::ipc::Request<'_>,
) -> Result<u64, String> {
    let tauri::ipc::InvokeBody::Raw(bytes) = request.body() else {
        return Err("文件数据块格式无效。".to_string());
    };
    let transfer_id = request
        .headers()
        .get("x-transfer-id")
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| "文件数据块缺少传输标识。".to_string())?;
    if bytes.is_empty() || bytes.len() > MAX_FILE_CHUNK_SIZE {
        return Err("文件数据块无效。".to_string());
    }
    let mut pending = remote
        .controller_file_receiver
        .lock()
        .map_err(|_| "文件接收状态不可用".to_string())?;
    let receiver = pending
        .as_mut()
        .filter(|receiver| receiver.metadata.transfer_id == transfer_id)
        .ok_or_else(|| "文件接收请求已过期。".to_string())?;
    append_partial_file_chunk(receiver, bytes)?;
    Ok(receiver.received_size)
}

/// 校验 SHA-256 并保存文件。大文件校验耗时较长，放到阻塞线程池执行，避免卡住界面主线程。
#[tauri::command]
pub async fn finish_controller_file_receive(
    app: AppHandle,
    remote: tauri::State<'_, Arc<RemoteState>>,
    transfer_id: String,
) -> Result<String, String> {
    let receiver = {
        let mut pending = remote
            .controller_file_receiver
            .lock()
            .map_err(|_| "文件接收状态不可用".to_string())?;
        if !pending
            .as_ref()
            .is_some_and(|receiver| receiver.metadata.transfer_id == transfer_id)
        {
            return Err("文件接收请求已过期。".to_string());
        }
        pending
            .take()
            .ok_or_else(|| "文件接收请求已过期。".to_string())?
    };
    tokio::task::spawn_blocking(move || finish_partial_file_receive(&app, receiver))
        .await
        .map_err(|error| format!("文件保存任务失败：{error}"))?
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
        .is_some_and(|receiver| receiver.metadata.transfer_id == transfer_id)
        && let Some(mut receiver) = pending.take()
    {
        discard_partial_file_receive(&mut receiver)?;
    }
    Ok(())
}

#[tauri::command]
pub fn suspend_controller_file_receive(
    remote: tauri::State<'_, Arc<RemoteState>>,
    transfer_id: String,
) -> Result<(), String> {
    let mut pending = remote
        .controller_file_receiver
        .lock()
        .map_err(|_| "文件接收状态不可用".to_string())?;
    if pending
        .as_ref()
        .is_some_and(|receiver| receiver.metadata.transfer_id == transfer_id)
    {
        pending.take();
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
    discovery: Mutex<Option<JoinHandle<()>>>,
    pending_approval: Mutex<Option<(String, oneshot::Sender<bool>)>>,
    pending_request: Mutex<Option<ConnectionRequest>>,
    pending_file_approval: Mutex<Option<(String, oneshot::Sender<bool>)>>,
    clipboard_sender: Mutex<Option<mpsc::UnboundedSender<String>>>,
    file_sender: Mutex<Option<mpsc::UnboundedSender<AgentFileCommand>>>,
    clipboard_last_text: Mutex<Option<String>>,
    clipboard_sync_enabled: AtomicBool,
    system_audio_sync_enabled: AtomicBool,
    controller_file_receiver: Mutex<Option<ControllerFileReceiver>>,
    outgoing_files: Mutex<Vec<(String, Arc<OutgoingFileSource>)>>,
    file_hash_cache: Mutex<Vec<CachedFileDigest>>,
    capture_display_id: Mutex<Option<u32>>,
    webrtc_factory: Mutex<Option<Arc<PeerConnectionFactory>>>,
    active_connection: AtomicBool,
    stopping: AtomicBool,
}

enum AgentFileCommand {
    Offer {
        transfer_id: String,
        source: Arc<OutgoingFileSource>,
    },
    Control {
        transfer_id: String,
        action: String,
    },
}

#[derive(Serialize, Deserialize)]
struct PartialTransferMetadata {
    transfer_id: String,
    file_name: String,
    expected_size: u64,
    sha256: String,
    updated_at: u64,
    /// 已 fsync 确认落盘的字节数。续传时只信任这个位置之前的数据。
    #[serde(default)]
    synced_size: Option<u64>,
    /// 发送端提供的每 64 MiB 分段 SHA-256，用于定位损坏段。旧记录为空。
    #[serde(default)]
    segment_hashes: Vec<String>,
}

struct ControllerFileReceiver {
    metadata: PartialTransferMetadata,
    file: Option<File>,
    partial_path: PathBuf,
    metadata_path: PathBuf,
    received_size: u64,
}

impl Drop for ControllerFileReceiver {
    /// 正常挂起（断线、暂停超时、切换传输）时把当前进度落盘并记为检查点，
    /// 这样正常中断不会损失进度；只有进程崩溃/断电才回退到上一个检查点。
    /// 完成或丢弃时 file 已被取走，不会再写记录。
    fn drop(&mut self) {
        if let Some(file) = self.file.take()
            && file.sync_data().is_ok()
        {
            self.metadata.synced_size = Some(self.received_size);
            if let Ok(now) = unix_timestamp() {
                self.metadata.updated_at = now;
            }
            let _ = write_partial_metadata(&self.metadata_path, &self.metadata);
        }
    }
}

struct OutgoingFileSource {
    path: PathBuf,
    file_name: String,
    size: u64,
    modified: Option<SystemTime>,
    sha256: String,
    segment_hashes: Vec<String>,
}

#[derive(Clone)]
struct FileDigest {
    sha256: String,
    segment_hashes: Vec<String>,
}

struct CachedFileDigest {
    path: PathBuf,
    size: u64,
    modified: Option<SystemTime>,
    digest: FileDigest,
}

/// 被控端正在发送给控制端的文件。数据由 Rust 直接从磁盘读取并写入 WebSocket，
/// 通过控制端的 file_ack 做滑动窗口流控。
struct AgentOutgoingFileTransfer {
    transfer_id: String,
    source: Arc<OutgoingFileSource>,
    file: Option<File>,
    sent: u64,
    acknowledged: u64,
    accepted: bool,
    paused: bool,
    completion_sent: bool,
    last_progress_event: u64,
}

impl AgentOutgoingFileTransfer {
    fn new(transfer_id: String, source: Arc<OutgoingFileSource>) -> Self {
        Self {
            transfer_id,
            source,
            file: None,
            sent: 0,
            acknowledged: 0,
            accepted: false,
            paused: false,
            completion_sent: false,
            last_progress_event: 0,
        }
    }

    fn size(&self) -> u64 {
        self.source.size
    }

    fn can_send(&self) -> bool {
        self.accepted
            && !self.paused
            && self.file.is_some()
            && self.sent < self.size()
            && self.sent.saturating_sub(self.acknowledged) < FILE_SEND_WINDOW
    }

    fn start(&mut self, resume_offset: u64) -> Result<(), String> {
        self.file = Some(open_outgoing_file_source(&self.source, resume_offset)?);
        self.sent = resume_offset;
        self.acknowledged = resume_offset;
        self.last_progress_event = resume_offset;
        self.accepted = true;
        self.paused = false;
        Ok(())
    }

    fn read_next_chunk(&mut self) -> Result<Vec<u8>, String> {
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| "文件发送句柄不可用。".to_string())?;
        read_outgoing_chunk(file, &self.source, self.sent)
    }
}

struct AgentIncomingFileTransfer {
    receiver: ControllerFileReceiver,
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
        #[serde(default)]
        system_audio_enabled: bool,
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
    FileTransferError {
        transfer_id: String,
        message: String,
    },
    FileTransferControl {
        transfer_id: String,
        action: String,
    },
    FileOffer {
        transfer_id: String,
        file_name: String,
        size: u64,
        sha256: String,
        #[serde(default)]
        segment_hashes: Vec<String>,
    },
    FileComplete {
        transfer_id: String,
    },
    FileTransferDecision {
        transfer_id: String,
        accepted: bool,
        #[serde(default)]
        resume_offset: u64,
    },
    FileAck {
        transfer_id: String,
        received: u64,
    },
    QualityAdjust {
        level: AdaptiveQuality,
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
pub struct DiscoveredDevice {
    name: String,
    address: String,
}

#[derive(Deserialize)]
struct DiscoveryPacket {
    kind: String,
    name: Option<String>,
    port: Option<u16>,
}

fn discovery_name(name: &str) -> Option<String> {
    let name: String = name.trim().chars().take(48).collect();
    if name.is_empty() || name.chars().any(char::is_control) {
        None
    } else {
        Some(name)
    }
}

fn discovery_broadcast_addresses() -> Result<Vec<Ipv4Addr>, String> {
    let interfaces = get_if_addrs().map_err(|error| format!("无法枚举局域网接口：{error}"))?;
    let mut broadcasts = HashSet::new();
    for interface in interfaces {
        let IfAddr::V4(address) = interface.addr else {
            continue;
        };
        if !(address.ip.is_private() || address.ip.is_link_local()) || address.ip.is_loopback() {
            continue;
        }
        if let Some(broadcast) = subnet_broadcast(address.ip, address.netmask) {
            broadcasts.insert(broadcast);
        }
    }
    if broadcasts.is_empty() {
        broadcasts.insert(Ipv4Addr::BROADCAST);
    }
    Ok(broadcasts.into_iter().collect())
}

fn local_device_ips() -> Result<HashSet<IpAddr>, String> {
    let mut addresses = get_if_addrs()
        .map_err(|error| format!("无法枚举本机网络接口：{error}"))?
        .into_iter()
        .map(|interface| match interface.addr {
            IfAddr::V4(address) => IpAddr::V4(address.ip),
            IfAddr::V6(address) => IpAddr::V6(address.ip),
        })
        .collect::<HashSet<_>>();
    addresses.insert(IpAddr::V4(Ipv4Addr::LOCALHOST));
    addresses.insert(IpAddr::V6(std::net::Ipv6Addr::LOCALHOST));
    Ok(addresses)
}

fn is_local_device_ip(address: IpAddr, local_addresses: &HashSet<IpAddr>) -> bool {
    match address {
        IpAddr::V4(ip) => local_addresses.contains(&IpAddr::V4(ip)),
        IpAddr::V6(ip) => {
            local_addresses.contains(&IpAddr::V6(ip))
                || ip
                    .to_ipv4_mapped()
                    .is_some_and(|mapped| local_addresses.contains(&IpAddr::V4(mapped)))
        }
    }
}

fn subnet_broadcast(ip: Ipv4Addr, netmask: Ipv4Addr) -> Option<Ipv4Addr> {
    let mask = u32::from(netmask);
    let broadcast = Ipv4Addr::from((u32::from(ip) & mask) | !mask);
    (broadcast != ip && !broadcast.is_broadcast()).then_some(broadcast)
}

async fn run_discovery_responder(app: AppHandle, port: u16) -> Result<(), String> {
    let socket = tokio::net::UdpSocket::bind(("0.0.0.0", DISCOVERY_PORT))
        .await
        .map_err(|error| format!("无法启动局域网发现服务：{error}"))?;
    let name = local_hostname()?;
    let response = serde_json::to_vec(&serde_json::json!({
        "kind": "lantern_response",
        "name": name,
        "port": port,
    }))
    .map_err(|error| format!("无法生成设备发现响应：{error}"))?;
    let mut buffer = [0_u8; 512];
    loop {
        let (length, peer) = socket
            .recv_from(&mut buffer)
            .await
            .map_err(|error| format!("局域网发现接收失败：{error}"))?;
        if !is_lan_address(peer.ip()) {
            continue;
        }
        let Ok(packet) = serde_json::from_slice::<DiscoveryPacket>(&buffer[..length]) else {
            continue;
        };
        if packet.kind == "lantern_probe"
            && let Err(error) = socket.send_to(&response, peer).await
        {
            let _ = app.emit("agent-status", format!("局域网发现响应失败：{error}"));
        }
    }
}

#[tauri::command]
pub async fn discover_lan_devices() -> Result<Vec<DiscoveredDevice>, String> {
    tokio::task::spawn_blocking(discover_lan_devices_blocking)
        .await
        .map_err(|error| format!("局域网扫描任务失败：{error}"))?
}

fn discover_lan_devices_blocking() -> Result<Vec<DiscoveredDevice>, String> {
    let local_addresses = local_device_ips()?;
    let std_socket = std::net::UdpSocket::bind(("0.0.0.0", 0))
        .map_err(|error| format!("无法启动局域网扫描：{error}"))?;
    std_socket
        .set_broadcast(true)
        .map_err(|error| format!("无法配置局域网广播：{error}"))?;
    std_socket
        .set_read_timeout(Some(Duration::from_millis(1200)))
        .map_err(|error| format!("无法配置局域网扫描超时：{error}"))?;
    let broadcasts = discovery_broadcast_addresses()?;
    let mut sent = 0;
    let mut send_errors = Vec::new();
    for address in broadcasts {
        match std_socket.send_to(
            br#"{"kind":"lantern_probe"}"#,
            SocketAddr::new(IpAddr::V4(address), DISCOVERY_PORT),
        ) {
            Ok(_) => sent += 1,
            Err(error) => send_errors.push(format!("{address}: {error}")),
        }
    }
    if sent == 0 {
        return Err(format!("局域网扫描发送失败：{}", send_errors.join("；")));
    }

    let mut devices = HashMap::new();
    let mut buffer = [0_u8; 512];
    loop {
        match std_socket.recv_from(&mut buffer) {
            Ok((length, peer)) => {
                if !is_lan_address(peer.ip()) || is_local_device_ip(peer.ip(), &local_addresses) {
                    continue;
                }
                let Ok(packet) = serde_json::from_slice::<DiscoveryPacket>(&buffer[..length])
                else {
                    continue;
                };
                if packet.kind != "lantern_response" {
                    continue;
                }
                let (Some(name), Some(port)) = (
                    packet.name.as_deref().and_then(discovery_name),
                    packet.port.filter(|port| *port != 0),
                ) else {
                    continue;
                };
                devices.insert(
                    SocketAddr::new(peer.ip(), port).to_string(),
                    DiscoveredDevice {
                        name,
                        address: SocketAddr::new(peer.ip(), port).to_string(),
                    },
                );
            }
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                break;
            }
            Err(error) => return Err(format!("局域网扫描接收失败：{error}")),
        }
    }
    Ok(devices.into_values().collect())
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FileTransferOffer {
    transfer_id: String,
    file_name: String,
    size: u64,
    sha256: String,
    received_bytes: u64,
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
pub fn is_local_device_address(address: String) -> Result<bool, String> {
    let local_addresses = local_device_ips()?;
    let resolved_addresses = address
        .to_socket_addrs()
        .map_err(|error| format!("无法校验设备地址：{error}"))?;
    Ok(resolved_addresses
        .map(|socket_address| socket_address.ip())
        .any(|ip| is_local_device_ip(ip, &local_addresses)))
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
    let discovery_app = app.clone();
    let discovery_handle = tokio::spawn(async move {
        if let Err(error) = run_discovery_responder(discovery_app.clone(), port).await {
            let _ = discovery_app.emit("agent-status", error);
        }
    });
    *remote
        .discovery
        .lock()
        .map_err(|_| "局域网发现服务状态不可用".to_string())? = Some(discovery_handle);

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
    if let Some(discovery) = remote
        .discovery
        .lock()
        .map_err(|_| "局域网发现服务状态不可用".to_string())?
        .take()
    {
        discovery.abort();
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
    ws.max_message_size(MAX_SIGNAL_MESSAGE_SIZE)
        .max_frame_size(MAX_SIGNAL_MESSAGE_SIZE)
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

    let (controller_name, video_quality, clipboard_enabled, system_audio_requested) =
        match timeout(Duration::from_secs(5), socket.recv()).await {
            Ok(Some(Ok(Message::Text(text)))) => match serde_json::from_str::<ClientMessage>(&text)
            {
                Ok(ClientMessage::Hello {
                    device_name,
                    quality,
                    clipboard_enabled,
                    system_audio_enabled,
                }) => {
                    let name: String = device_name.trim().chars().take(48).collect();
                    let name = if name.is_empty() {
                        "局域网控制端".to_string()
                    } else {
                        name
                    };
                    (name, quality, clipboard_enabled, system_audio_enabled)
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
    let _ = append_diagnostic_log(&state.app, DiagnosticEvent::ConnectionRequestReceived);
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
        let _ = append_diagnostic_log(&state.app, DiagnosticEvent::ConnectionRequestRejected);
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

    let _ = append_diagnostic_log(&state.app, DiagnosticEvent::ConnectionRequestApproved);
    let _ = state.app.emit("agent-peer-identity", peer.ip().to_string());
    let _ = append_diagnostic_log(&state.app, DiagnosticEvent::AgentConnectionApproved);
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
        system_audio_requested,
        &controller_name,
    )
    .await
    {
        Ok(()) => None,
        Err(error) => {
            let _ = append_diagnostic_log(&state.app, DiagnosticEvent::AgentConnectionFailed);
            let _ = send_signal(
                &mut socket,
                serde_json::json!({"type": "error", "message": error}),
            )
            .await;
            let _ = socket.send(Message::Close(None)).await;
            Some(error)
        }
    };

    if session_error.is_none() {
        let _ = append_diagnostic_log(&state.app, DiagnosticEvent::AgentConnectionClosed);
    }
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
    AudioStatus(String),
    AudioRtpStats {
        packets: u64,
        bytes: u64,
    },
    AudioRtpMissing,
    VideoFramesSubmitted(u64),
    StatsReadFailed,
    InputMessagesProcessed(u64),
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
            let mut audio_report_tick = 0_u8;
            let mut audio_log_tick = 0_u8;
            while !stopping.load(Ordering::Acquire) {
                thread::sleep(Duration::from_secs(1));
                if stopping.load(Ordering::Acquire) {
                    break;
                }
                audio_report_tick = (audio_report_tick + 1) % 3;
                audio_log_tick = (audio_log_tick + 1) % 15;
                if let Ok(stats) = peer_connection.get_stats() {
                    if let Some(video) = stats
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
                    if let Some(audio) = stats
                        .outbound_rtp
                        .iter()
                        .find(|stream| stream.kind == StreamKind::Audio)
                    {
                        if audio_report_tick == 0 {
                            let codec = audio.codec_mime_type.as_deref().unwrap_or("codec pending");
                            let _ = signals.send(WebRtcSignal::AudioStatus(format!(
                                "音频发送链路：{} 包，{} 字节（{}）。",
                                audio.packets_sent, audio.bytes_sent, codec
                            )));
                        }
                        if audio_log_tick == 0 {
                            let _ = signals.send(WebRtcSignal::AudioRtpStats {
                                packets: audio.packets_sent,
                                bytes: audio.bytes_sent,
                            });
                        }
                    } else if audio_report_tick == 0 {
                        let _ = signals.send(WebRtcSignal::AudioStatus(
                            "WebRTC 尚未创建音频 RTP 发送流。".to_string(),
                        ));
                        if audio_log_tick == 0 {
                            let _ = signals.send(WebRtcSignal::AudioRtpMissing);
                        }
                    }
                } else {
                    if audio_log_tick == 0 {
                        let _ = signals.send(WebRtcSignal::StatsReadFailed);
                    }
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
        display_id: Option<u32>,
    ) {
        if self.thread.is_some() {
            return;
        }
        let stopping = Arc::clone(&self.stopping);
        self.thread = Some(thread::spawn(move || {
            let mut submitted_frames = 0_u64;
            while !stopping.load(Ordering::Acquire) {
                let started = std::time::Instant::now();
                match capture_bgra(settings.max_width, display_id) {
                    Ok((frame, frame_width, frame_height)) => {
                        if let Err(error) =
                            track.push_frame(VideoFrame::new(&frame, frame_width, frame_height))
                        {
                            let _ = signals.send(WebRtcSignal::CaptureError(error.to_string()));
                            break;
                        }
                        submitted_frames += 1;
                        if submitted_frames % 300 == 0 {
                            let _ =
                                signals.send(WebRtcSignal::VideoFramesSubmitted(submitted_frames));
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

struct AudioCaptureWorker {
    stopping: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

impl AudioCaptureWorker {
    fn new() -> Self {
        Self {
            stopping: Arc::new(AtomicBool::new(false)),
            thread: None,
        }
    }

    fn start(
        &mut self,
        track: Arc<AudioTrack>,
        app: AppHandle,
        signals: mpsc::UnboundedSender<WebRtcSignal>,
    ) {
        if self.thread.is_some() {
            return;
        }
        let stopping = Arc::clone(&self.stopping);
        self.thread = Some(thread::spawn(move || {
            let mut stream = match flexaudio::open(flexaudio::StreamConfig {
                kind: flexaudio::SourceKind::SystemLoopback,
                exclude_self: true,
                ring_capacity_chunks: 5,
                ..Default::default()
            }) {
                Ok(stream) => stream,
                Err(error) => {
                    let status = format!("无法捕获被控端系统声音：{error}");
                    let _ = append_diagnostic_log(&app, DiagnosticEvent::AudioCaptureFailed);
                    let _ = signals.send(WebRtcSignal::AudioStatus(status));
                    return;
                }
            };
            if let Err(error) = stream.start() {
                let status = format!("无法启动系统声音采集：{error}");
                let _ = append_diagnostic_log(&app, DiagnosticEvent::AudioCaptureFailed);
                let _ = signals.send(WebRtcSignal::AudioStatus(status));
                return;
            }
            if stopping.load(Ordering::Acquire) {
                stream.stop();
                return;
            }

            let status = "系统声音采集已启动，正在等待声音。".to_string();
            let _ = append_diagnostic_log(&app, DiagnosticEvent::AudioCaptureStarted);
            let _ = app.emit("agent-audio-status", &status);
            let _ = signals.send(WebRtcSignal::AudioStatus(status));
            let mut audio_detected = false;
            let mut pushed_frames = 0_u64;
            while !stopping.load(Ordering::Acquire) {
                if let Some(chunk) = stream.poll_chunk() {
                    let has_audio = chunk.peak >= 0.001;
                    if has_audio != audio_detected {
                        audio_detected = has_audio;
                        let event = if audio_detected {
                            DiagnosticEvent::AudioCaptureSignalDetected
                        } else {
                            DiagnosticEvent::AudioCaptureSignalMissing
                        };
                        let _ = append_diagnostic_log_values(
                            &app,
                            event,
                            Some((chunk.peak.max(0.0) * 1000.0) as u64),
                            None,
                        );
                        let status = if audio_detected {
                            "被控端已检测到系统声音，正在发送。".to_string()
                        } else {
                            "系统声音采集已运行，但当前未检测到声音。".to_string()
                        };
                        let _ = app.emit("agent-audio-status", &status);
                        let _ = signals.send(WebRtcSignal::AudioStatus(status));
                    }
                    let pcm = system_audio_to_pcm(&chunk.data);
                    if let Err(error) = track.push_frame(AudioFrame::new(&pcm, 48_000, 2)) {
                        let status = format!("系统声音传输停止：{error}");
                        let _ = append_diagnostic_log(&app, DiagnosticEvent::AudioCaptureFailed);
                        let _ = app.emit("agent-audio-status", &status);
                        let _ = signals.send(WebRtcSignal::AudioStatus(status));
                        break;
                    }
                    pushed_frames += 1;
                    if pushed_frames % 500 == 0 {
                        let _ = append_diagnostic_log_values(
                            &app,
                            DiagnosticEvent::AudioFramesSubmitted,
                            Some(pushed_frames),
                            Some(u64::from(audio_detected)),
                        );
                    }
                    if pushed_frames % 100 == 0 {
                        let status = format!(
                            "已向 WebRTC 音轨提交 {pushed_frames} 个系统音频帧，等待 RTP 发送统计。"
                        );
                        let _ = app.emit("agent-audio-status", &status);
                        let _ = signals.send(WebRtcSignal::AudioStatus(status));
                    }
                } else {
                    thread::sleep(Duration::from_millis(2));
                }
            }
            stream.stop();
        }));
    }
}

impl Drop for AudioCaptureWorker {
    fn drop(&mut self) {
        self.stopping.store(true, Ordering::Release);
        self.thread.take();
    }
}

async fn run_webrtc_session(
    socket: &mut WebSocket,
    state: &ServerState,
    quality: VideoQuality,
    clipboard_enabled: bool,
    system_audio_requested: bool,
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
    let _ = append_diagnostic_log(&state.app, DiagnosticEvent::SignalingOfferReceived);

    let selected_display_id = *state
        .remote
        .capture_display_id
        .lock()
        .map_err(|_| "显示器选择状态不可用。".to_string())?;
    let (width, height, origin_x, origin_y) =
        tokio::task::spawn_blocking(move || capture_dimensions(selected_display_id))
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
            if let Err(error) = apply_input(
                &mut enigo,
                command,
                width,
                height,
                origin_x,
                origin_y,
                &mut input_state,
            ) {
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
    let input_message_count = Arc::new(AtomicU64::new(0));
    let input_count_for_peer = Arc::clone(&input_message_count);
    let signals_for_input = signal_sender.clone();
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
            let input_message_count = Arc::clone(&input_count_for_peer);
            let signals_for_input = signals_for_input.clone();
            channel.on_message(move |data, binary| {
                if binary {
                    return;
                }
                if let Ok(text) = std::str::from_utf8(data)
                    && let Ok(command) = serde_json::from_str::<ClientMessage>(text)
                    && is_input_message(&command)
                {
                    let count = input_message_count.fetch_add(1, Ordering::Relaxed) + 1;
                    if count % 1000 == 0 {
                        let _ = signals_for_input.send(WebRtcSignal::InputMessagesProcessed(count));
                    }
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
    let mut adaptive_quality = AdaptiveQuality::Normal;
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
    let _ = append_diagnostic_log(&state.app, DiagnosticEvent::VideoTrackCreated);
    let system_audio_enabled = system_audio_requested
        && state
            .remote
            .system_audio_sync_enabled
            .load(Ordering::Acquire);
    let mut audio_setup_error = None;
    let audio_track = if system_audio_enabled {
        let mut options = AudioTrackOptions::default();
        options.source = AudioTrackSource::LocalPush;
        options.echo_cancellation = Some(false);
        options.noise_suppression = Some(false);
        options.auto_gain_control = Some(false);
        options.high_pass_filter = Some(false);
        match state
            .webrtc_factory
            .create_audio_track_with_options("lantern-system-audio", options)
        {
            Ok(track) => Some(Arc::new(track)),
            Err(error) => {
                audio_setup_error = Some(format!("无法创建系统声音轨道：{error}"));
                None
            }
        }
    } else {
        None
    };
    if audio_track.is_some() {
        let _ = append_diagnostic_log(&state.app, DiagnosticEvent::AudioTrackCreated);
    }
    for transceiver in peer_connection.transceivers() {
        match transceiver.kind() {
            MediaKind::Video => {
                transceiver
                    .set_track(&video_track)
                    .map_err(|error| format!("无法绑定屏幕视频轨道：{error}"))?;
                transceiver
                    .set_direction(TransceiverDirection::SendOnly)
                    .map_err(|error| format!("无法启用屏幕视频发送：{error}"))?;
                transceiver
                    .set_codec_preferences(&[VideoCodec::H264, VideoCodec::Vp8])
                    .map_err(|error| format!("无法配置 WebRTC 视频编码格式：{error}"))?;
            }
            MediaKind::Audio => {
                if let Some(track) = &audio_track {
                    transceiver
                        .set_track(track)
                        .map_err(|error| format!("无法绑定系统声音轨道：{error}"))?;
                    transceiver
                        .set_direction(TransceiverDirection::SendOnly)
                        .map_err(|error| format!("无法启用系统声音发送：{error}"))?;
                }
            }
            MediaKind::Unknown => {}
        }
    }
    let answer = peer_connection
        .create_answer()
        .map_err(|error| format!("无法创建 WebRTC answer：{error}"))?;
    if system_audio_requested {
        let mut in_audio_section = false;
        let audio_sendonly = answer.sdp.lines().any(|line| {
            if line.starts_with("m=") {
                in_audio_section = line.starts_with("m=audio ");
            }
            in_audio_section && line.trim() == "a=sendonly"
        });
        let status = if !system_audio_enabled {
            "控制端请求了系统声音，但被控端的共享设置未启用。".to_string()
        } else if audio_sendonly {
            "WebRTC 音频已协商为发送方向，等待音频 RTP 数据。".to_string()
        } else {
            "WebRTC answer 未协商出音频发送方向，无法发送系统声音。".to_string()
        };
        let event = if audio_sendonly && system_audio_enabled {
            DiagnosticEvent::AudioNegotiationSendOnly
        } else {
            DiagnosticEvent::AudioNegotiationMissing
        };
        let _ = append_diagnostic_log(&state.app, event);
        send_signal(
            socket,
            serde_json::json!({"type": "audio_status", "status": status}),
        )
        .await
        .map_err(|error| format!("发送音频协商状态失败：{error}"))?;
    } else {
        let _ = append_diagnostic_log(&state.app, DiagnosticEvent::AudioNegotiationNotRequested);
    }
    peer_connection
        .set_local_description(&answer)
        .map_err(|error| format!("无法应用 WebRTC answer：{error}"))?;
    send_signal(
        socket,
        serde_json::json!({"type": "answer", "sdp": answer.sdp}),
    )
    .await
    .map_err(|error| format!("无法发送 WebRTC answer：{error}"))?;
    let _ = append_diagnostic_log(&state.app, DiagnosticEvent::WebRtcAnswerCreated);

    let mut capture_worker = CaptureWorker::new();
    let mut audio_capture_worker = AudioCaptureWorker::new();
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
    let mut agent_file_transfer: Option<AgentOutgoingFileTransfer> = None;
    let mut agent_file_finish: Option<(String, String, u64, JoinHandle<Result<String, String>>)> =
        None;
    let mut agent_incoming_file_transfer: Option<AgentIncomingFileTransfer> = None;
    let mut file_approval_receiver: Option<oneshot::Receiver<bool>> = None;
    let mut file_approval_deadline = None;
    let mut file_receive_deadline = None;
    let mut peer_disconnect_deadline = None;
    let mut received_ice_candidates = 0_u64;
    let mut sent_ice_candidates = 0_u64;

    loop {
        tokio::select! {
            incoming = socket.recv() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        match serde_json::from_str::<ClientMessage>(&text) {
                            Ok(ClientMessage::IceCandidate { candidate, sdp_mid, sdp_mline_index }) => {
                                received_ice_candidates += 1;
                                let _ = append_diagnostic_log_values(
                                    &state.app,
                                    DiagnosticEvent::IceCandidateReceived,
                                    Some(received_ice_candidates),
                                    None,
                                );
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
                                    let _ = append_diagnostic_log_values(
                                        &state.app,
                                        DiagnosticEvent::ClipboardTextReceived,
                                        Some(text.len() as u64),
                                        None,
                                    );
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
                                sha256,
                                segment_hashes,
                            }) => {
                                let _ = append_diagnostic_log_values(
                                    &state.app,
                                    DiagnosticEvent::FileTransferOfferReceived,
                                    Some(size),
                                    None,
                                );
                                let valid_offer = is_valid_file_offer(
                                    &transfer_id,
                                    &file_name,
                                    size,
                                    &sha256,
                                ) && is_valid_segment_hashes(size, &segment_hashes);
                                if !valid_offer
                                    || agent_file_transfer.is_some()
                                    || agent_incoming_file_transfer.is_some()
                                    || agent_file_finish.is_some()
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
                                    let partial_receiver = match open_partial_file_receiver(
                                        &state.app,
                                        transfer_id.clone(),
                                        file_name.clone(),
                                        size,
                                        sha256,
                                        segment_hashes,
                                    ) {
                                        Ok(receiver) => receiver,
                                        Err(error) => {
                                            send_signal(
                                                socket,
                                                serde_json::json!({
                                                    "type": "file_transfer_error",
                                                    "transfer_id": transfer_id,
                                                    "message": error,
                                                }),
                                            )
                                            .await
                                            .map_err(|error| format!("发送文件续传错误失败：{error}"))?;
                                            continue;
                                        }
                                    };
                                    let received_bytes = partial_receiver.received_size;
                                    let (sender, approval_receiver) = oneshot::channel();
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
                                                sha256: partial_receiver.metadata.sha256.clone(),
                                                received_bytes,
                                            },
                                        )
                                        .map_err(|error| {
                                            format!("无法显示文件接收请求：{error}")
                                        })?;
                                    agent_incoming_file_transfer = Some(AgentIncomingFileTransfer {
                                        receiver: partial_receiver,
                                        accepted: false,
                                        _approval_guard: approval_guard,
                                    });
                                    file_approval_receiver = Some(approval_receiver);
                                    file_approval_deadline = Some(
                                        tokio::time::Instant::now()
                                            + Duration::from_secs(90),
                                    );
                                }
                            }
                            Ok(ClientMessage::FileComplete { transfer_id }) => {
                                let Some(incoming) = agent_incoming_file_transfer.take() else {
                                    send_signal(socket, serde_json::json!({
                                        "type": "file_transfer_error",
                                        "transfer_id": transfer_id,
                                        "message": "文件接收请求已过期。",
                                    }))
                                    .await
                                    .map_err(|error| format!("发送文件错误状态失败：{error}"))?;
                                    continue;
                                };
                                if !incoming.accepted
                                    || incoming.receiver.metadata.transfer_id != transfer_id
                                    || incoming.receiver.received_size
                                        != incoming.receiver.metadata.expected_size
                                {
                                    let expected_transfer_id =
                                        incoming.receiver.metadata.transfer_id.clone();
                                    agent_incoming_file_transfer = Some(incoming);
                                    send_signal(socket, serde_json::json!({
                                        "type": "file_transfer_error",
                                        "transfer_id": expected_transfer_id,
                                        "message": "文件数据尚未完整，续传进度已保留。",
                                    }))
                                    .await
                                    .map_err(|error| format!("发送文件错误状态失败：{error}"))?;
                                    continue;
                                }
                                file_receive_deadline = None;
                                let transfer_id = incoming.receiver.metadata.transfer_id.clone();
                                let file_name = incoming.receiver.metadata.file_name.clone();
                                let received_size = incoming.receiver.metadata.expected_size;
                                let app = state.app.clone();
                                let receiver = incoming.receiver;
                                // SHA-256 校验和保存放到阻塞线程池，期间信令循环继续处理键鼠和 WebRTC 消息。
                                agent_file_finish = Some((
                                    transfer_id,
                                    file_name,
                                    received_size,
                                    tokio::task::spawn_blocking(move || {
                                        finish_partial_file_receive(&app, receiver)
                                    }),
                                ));
                            }
                            Ok(ClientMessage::FileTransferDecision {
                                transfer_id,
                                accepted,
                                resume_offset,
                            }) => {
                                match agent_file_transfer.as_mut() {
                                    Some(transfer) if transfer.transfer_id == transfer_id => {
                                        let size = transfer.size();
                                        if accepted && resume_offset > size {
                                            return Err("续传进度超过文件长度".to_string());
                                        }
                                        let start_error = if accepted {
                                            transfer.start(resume_offset).err()
                                        } else {
                                            None
                                        };
                                        let _ = append_diagnostic_log_values(
                                            &state.app,
                                            if accepted {
                                                DiagnosticEvent::FileTransferAccepted
                                            } else {
                                                DiagnosticEvent::FileTransferRejected
                                            },
                                            Some(resume_offset),
                                            Some(size),
                                        );
                                        if let Some(error) = start_error {
                                            agent_file_transfer = None;
                                            let _ = append_diagnostic_log(
                                                &state.app,
                                                DiagnosticEvent::FileTransferFailed,
                                            );
                                            let _ = state.app.emit(
                                                "agent-file-transfer-decision",
                                                serde_json::json!({
                                                    "transferId": transfer_id,
                                                    "accepted": false,
                                                    "error": error,
                                                }),
                                            );
                                            send_signal(socket, serde_json::json!({
                                                "type": "file_transfer_error",
                                                "transfer_id": transfer_id,
                                                "message": error,
                                            }))
                                            .await
                                            .map_err(|error| format!("发送文件传输错误失败：{error}"))?;
                                            continue;
                                        }
                                        state.app.emit(
                                            "agent-file-transfer-decision",
                                            serde_json::json!({
                                                "transferId": transfer_id,
                                                "accepted": accepted,
                                                "resumeOffset": resume_offset,
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
                            Ok(ClientMessage::FileTransferControl { transfer_id, action }) => {
                                if !matches!(action.as_str(), "pause" | "resume" | "cancel") {
                                    send_signal(socket, serde_json::json!({
                                        "type": "file_transfer_error",
                                        "transfer_id": transfer_id,
                                        "message": "文件传输控制请求无效。",
                                    }))
                                    .await
                                    .map_err(|error| format!("发送文件传输错误失败：{error}"))?;
                                    continue;
                                }
                                if agent_incoming_file_transfer
                                    .as_ref()
                                    .is_some_and(|incoming| incoming.receiver.metadata.transfer_id == transfer_id)
                                {
                                    let control_event = match action.as_str() {
                                        "pause" => DiagnosticEvent::FileTransferPaused,
                                        "resume" => DiagnosticEvent::FileTransferResumed,
                                        "cancel" => DiagnosticEvent::FileTransferCancelled,
                                        _ => unreachable!(),
                                    };
                                    let _ = append_diagnostic_log(
                                        &state.app,
                                        control_event,
                                    );
                                    match action.as_str() {
                                        "pause" => file_receive_deadline = None,
                                        "resume" => {
                                            file_receive_deadline = Some(
                                                tokio::time::Instant::now()
                                                    + Duration::from_secs(30),
                                            );
                                        }
                                        "cancel" => {
                                            if let Some(mut incoming) = agent_incoming_file_transfer.take() {
                                                discard_partial_file_receive(&mut incoming.receiver)?;
                                            }
                                            file_receive_deadline = None;
                                        }
                                        _ => unreachable!(),
                                    }
                                    let _ = state.app.emit(
                                        "agent-file-transfer-control",
                                        serde_json::json!({
                                            "transferId": transfer_id,
                                            "action": action,
                                        }),
                                    );
                                } else if agent_file_transfer
                                    .as_ref()
                                    .is_some_and(|transfer| transfer.transfer_id == transfer_id)
                                {
                                    let control_event = match action.as_str() {
                                        "pause" => DiagnosticEvent::FileTransferPaused,
                                        "resume" => DiagnosticEvent::FileTransferResumed,
                                        "cancel" => DiagnosticEvent::FileTransferCancelled,
                                        _ => unreachable!(),
                                    };
                                    let _ = append_diagnostic_log(
                                        &state.app,
                                        control_event,
                                    );
                                    let _ = state.app.emit(
                                        "agent-file-transfer-control",
                                        serde_json::json!({
                                            "transferId": transfer_id,
                                            "action": action,
                                        }),
                                    );
                                    if action == "cancel" {
                                        agent_file_transfer = None;
                                    } else if let Some(transfer) = agent_file_transfer.as_mut() {
                                        transfer.paused = action == "pause";
                                    }
                                } else {
                                    send_signal(socket, serde_json::json!({
                                        "type": "file_transfer_error",
                                        "transfer_id": transfer_id,
                                        "message": "文件传输请求已过期。",
                                    }))
                                    .await
                                    .map_err(|error| format!("发送文件传输错误失败：{error}"))?;
                                }
                            }
                            Ok(ClientMessage::FileTransferComplete { transfer_id }) => {
                                if agent_file_transfer.as_ref().is_some_and(|transfer| {
                                    transfer.transfer_id == transfer_id
                                        && transfer.accepted
                                        && transfer.sent == transfer.size()
                                }) {
                                    let completed_size = agent_file_transfer
                                        .as_ref()
                                        .map(AgentOutgoingFileTransfer::size)
                                        .unwrap_or_default();
                                    let _ = state.app.emit(
                                        "agent-file-transfer-complete",
                                        serde_json::json!({
                                            "transferId": transfer_id,
                                        }),
                                    );
                                    agent_file_transfer = None;
                                    let _ = append_diagnostic_log_values(
                                        &state.app,
                                        DiagnosticEvent::FileTransferCompleted,
                                        Some(completed_size),
                                        None,
                                    );
                                    let _ = state.app.emit(
                                        "agent-file-transfer-status",
                                        "文件已成功送达控制端。",
                                    );
                                }
                            }
                            Ok(ClientMessage::FileAck { transfer_id, received }) => {
                                if let Some(transfer) = agent_file_transfer
                                    .as_mut()
                                    .filter(|transfer| transfer.transfer_id == transfer_id)
                                {
                                    transfer.acknowledged =
                                        transfer.acknowledged.max(received.min(transfer.sent));
                                }
                            }
                            Ok(ClientMessage::FileTransferError { transfer_id, message }) => {
                                if agent_file_transfer
                                    .as_ref()
                                    .is_some_and(|transfer| transfer.transfer_id == transfer_id)
                                {
                                    agent_file_transfer = None;
                                    let _ = append_diagnostic_log(
                                        &state.app,
                                        DiagnosticEvent::FileTransferFailed,
                                    );
                                    let _ = state.app.emit(
                                        "agent-file-transfer-error",
                                        serde_json::json!({
                                            "transferId": transfer_id,
                                            "message": message,
                                        }),
                                    );
                                }
                            }
                            Ok(ClientMessage::QualityAdjust { level }) => {
                                if adaptive_quality != level {
                                    let (minimum, maximum) =
                                        bitrate_for_network(video_settings, level);
                                    peer_connection
                                        .set_bitrate(None, Some(minimum), Some(maximum))
                                        .map_err(|error| {
                                            format!("无法调整 WebRTC 视频码率：{error}")
                                        })?;
                                    adaptive_quality = level;
                                    send_signal(
                                        socket,
                                        serde_json::json!({
                                            "type": "quality_status",
                                            "level": if level == AdaptiveQuality::Constrained {
                                                "constrained"
                                            } else {
                                                "normal"
                                            },
                                        }),
                                    )
                                    .await
                                    .map_err(|error| format!("发送画质调整状态失败：{error}"))?;
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
                        let previous_size = incoming.receiver.received_size;
                        append_partial_file_chunk(&mut incoming.receiver, &chunk)?;
                        if incoming.receiver.received_size / FILE_PROGRESS_LOG_STEP
                            > previous_size / FILE_PROGRESS_LOG_STEP
                        {
                            let _ = append_diagnostic_log_values(
                                &state.app,
                                DiagnosticEvent::FileTransferProgress,
                                Some(incoming.receiver.received_size),
                                Some(incoming.receiver.metadata.expected_size),
                            );
                        }
                        if incoming.receiver.received_size / FILE_PROGRESS_EVENT_STEP
                            > previous_size / FILE_PROGRESS_EVENT_STEP
                            || incoming.receiver.received_size
                                == incoming.receiver.metadata.expected_size
                        {
                            let _ = state.app.emit(
                                "agent-file-transfer-progress",
                                serde_json::json!({
                                    "transferId": incoming.receiver.metadata.transfer_id.clone(),
                                    "receivedBytes": incoming.receiver.received_size,
                                }),
                            );
                        }
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
                    let _ = append_diagnostic_log_values(
                        &state.app,
                        if accepted {
                            DiagnosticEvent::FileTransferAccepted
                        } else {
                            DiagnosticEvent::FileTransferRejected
                        },
                        Some(if accepted {
                            incoming.receiver.received_size
                        } else {
                            0
                        }),
                        Some(incoming.receiver.metadata.expected_size),
                    );
                    if accepted {
                        file_receive_deadline =
                            Some(tokio::time::Instant::now() + Duration::from_secs(30));
                    }
                    send_signal(
                        socket,
                        serde_json::json!({
                            "type": "file_transfer_decision",
                            "transfer_id": incoming.receiver.metadata.transfer_id,
                            "accepted": accepted,
                            "resume_offset": if accepted { incoming.receiver.received_size } else { 0 },
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
                    let _ = append_diagnostic_log(
                        &state.app,
                        DiagnosticEvent::FileTransferRejected,
                    );
                    send_signal(
                        socket,
                        serde_json::json!({
                            "type": "file_transfer_decision",
                            "transfer_id": incoming.receiver.metadata.transfer_id,
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
                    let _ = append_diagnostic_log_values(
                        &state.app,
                        DiagnosticEvent::FileTransferFailed,
                        Some(incoming.receiver.received_size),
                        Some(incoming.receiver.metadata.expected_size),
                    );
                    send_signal(socket, serde_json::json!({
                        "type": "file_transfer_error",
                        "transfer_id": incoming.receiver.metadata.transfer_id,
                        "message": "文件传输 30 秒无数据，续传进度已保留。",
                    }))
                    .await
                    .map_err(|error| format!("发送文件超时状态失败：{error}"))?;
                }
            }
            signal = signal_receiver.recv() => {
                match signal {
                    Some(WebRtcSignal::Candidate(candidate)) => {
                        sent_ice_candidates += 1;
                        let _ = append_diagnostic_log_values(
                            &state.app,
                            DiagnosticEvent::IceCandidateSent,
                            Some(sent_ice_candidates),
                            None,
                        );
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
                        peer_disconnect_deadline = None;
                        let _ = append_diagnostic_log(
                            &state.app,
                            DiagnosticEvent::WebRtcStateConnected,
                        );
                        let _ = append_diagnostic_log(
                            &state.app,
                            DiagnosticEvent::AgentConnectionEstablished,
                        );
                        send_signal(socket, serde_json::json!({"type": "connected"}))
                            .await
                            .map_err(|error| format!("发送连接状态失败：{error}"))?;
                        let _ = state.app.emit("agent-connection-state", true);
                        let _ = state
                            .app
                            .emit("agent-status", format!("控制端「{controller_name}」已连接。"));
                        capture_worker.start(
                            Arc::clone(&video_track),
                            signal_sender.clone(),
                            video_settings,
                            selected_display_id,
                        );
                        if let Some(track) = &audio_track {
                            audio_capture_worker.start(
                                Arc::clone(track),
                                state.app.clone(),
                                signal_sender.clone(),
                            );
                        } else if let Some(status) = audio_setup_error.take() {
                            let _ = append_diagnostic_log(
                                &state.app,
                                DiagnosticEvent::AudioCaptureFailed,
                            );
                            let _ = state.app.emit("agent-audio-status", &status);
                            let _ = send_signal(
                                socket,
                                serde_json::json!({
                                    "type": "audio_status",
                                    "status": status,
                                }),
                            )
                            .await;
                        } else if system_audio_requested {
                            let status = "被控端未启用系统声音共享。".to_string();
                            let _ = state.app.emit("agent-audio-status", &status);
                            let _ = send_signal(
                                socket,
                                serde_json::json!({
                                    "type": "audio_status",
                                    "status": status,
                                }),
                            )
                            .await;
                        }
                        stats_worker.start(Arc::clone(&peer_connection), signal_sender.clone());
                    }
                    Some(WebRtcSignal::State(PeerConnectionState::Failed | PeerConnectionState::Closed)) => {
                        let _ = append_diagnostic_log(
                            &state.app,
                            DiagnosticEvent::WebRtcStateFailed,
                        );
                        return Err("WebRTC 媒体连接已失败或关闭".to_string());
                    }
                    Some(WebRtcSignal::State(PeerConnectionState::Disconnected)) => {
                        if peer_disconnect_deadline.is_none() {
                            let _ = append_diagnostic_log(
                                &state.app,
                                DiagnosticEvent::WebRtcStateDisconnected,
                            );
                            let _ = append_diagnostic_log(
                                &state.app,
                                DiagnosticEvent::AgentConnectionInterrupted,
                            );
                            peer_disconnect_deadline =
                                Some(tokio::time::Instant::now() + Duration::from_secs(5));
                        }
                    }
                    Some(WebRtcSignal::State(_)) => {}
                    Some(WebRtcSignal::CaptureError(error)) => {
                        let _ = append_diagnostic_log(
                            &state.app,
                            DiagnosticEvent::VideoCaptureFailed,
                        );
                        return Err(format!("屏幕采集或视频编码失败：{error}"));
                    }
                    Some(WebRtcSignal::VideoFramesSubmitted(frames)) => {
                        let _ = append_diagnostic_log_values(
                            &state.app,
                            DiagnosticEvent::VideoFramesSubmitted,
                            Some(frames),
                            None,
                        );
                    }
                    Some(WebRtcSignal::AudioStatus(status)) => {
                        let _ = state.app.emit("agent-audio-status", &status);
                        send_signal(
                            socket,
                            serde_json::json!({
                                "type": "audio_status",
                                "status": status,
                            }),
                        )
                        .await
                        .map_err(|error| format!("发送系统声音状态失败：{error}"))?;
                    }
                    Some(WebRtcSignal::AudioRtpStats { packets, bytes }) => {
                        let _ = append_diagnostic_log_values(
                            &state.app,
                            DiagnosticEvent::AudioOutboundRtpStats,
                            Some(packets),
                            Some(bytes),
                        );
                    }
                    Some(WebRtcSignal::AudioRtpMissing) => {
                        let _ = append_diagnostic_log(
                            &state.app,
                            DiagnosticEvent::AudioOutboundRtpMissing,
                        );
                    }
                    Some(WebRtcSignal::StatsReadFailed) => {
                        let _ = append_diagnostic_log(
                            &state.app,
                            DiagnosticEvent::WebRtcStatsReadFailed,
                        );
                    }
                    Some(WebRtcSignal::InputMessagesProcessed(count)) => {
                        let _ = append_diagnostic_log_values(
                            &state.app,
                            DiagnosticEvent::InputMessagesProcessed,
                            Some(count),
                            None,
                        );
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
                    let _ = append_diagnostic_log_values(
                        &state.app,
                        DiagnosticEvent::ClipboardTextSent,
                        Some(text.len() as u64),
                        None,
                    );
                    send_signal(socket, serde_json::json!({
                        "type": "clipboard_offer",
                        "text": text,
                    }))
                    .await
                    .map_err(|error| format!("发送剪贴板内容失败：{error}"))?;
                }
            }
            _ = std::future::ready(()), if agent_file_transfer.as_ref().is_some_and(AgentOutgoingFileTransfer::can_send) => {
                let Some(transfer) = agent_file_transfer.as_mut() else {
                    continue;
                };
                let chunk = match transfer.read_next_chunk() {
                    Ok(chunk) => chunk,
                    Err(error) => {
                        let transfer_id = transfer.transfer_id.clone();
                        agent_file_transfer = None;
                        let _ = append_diagnostic_log(&state.app, DiagnosticEvent::FileTransferFailed);
                        let _ = state.app.emit(
                            "agent-file-transfer-error",
                            serde_json::json!({
                                "transferId": transfer_id,
                                "message": error,
                                "local": true,
                            }),
                        );
                        send_signal(socket, serde_json::json!({
                            "type": "file_transfer_error",
                            "transfer_id": transfer_id,
                            "message": error,
                        }))
                        .await
                        .map_err(|error| format!("发送文件传输错误失败：{error}"))?;
                        continue;
                    }
                };
                let chunk_len = chunk.len() as u64;
                socket
                    .send(Message::Binary(chunk.into()))
                    .await
                    .map_err(|error| format!("发送文件数据失败：{error}"))?;
                transfer.sent += chunk_len;
                let sent = transfer.sent;
                let size = transfer.size();
                if sent / FILE_PROGRESS_LOG_STEP > (sent - chunk_len) / FILE_PROGRESS_LOG_STEP {
                    let _ = append_diagnostic_log_values(
                        &state.app,
                        DiagnosticEvent::FileTransferProgress,
                        Some(sent),
                        Some(size),
                    );
                }
                if sent - transfer.last_progress_event >= FILE_PROGRESS_EVENT_STEP || sent == size {
                    transfer.last_progress_event = sent;
                    let _ = state.app.emit(
                        "agent-file-transfer-progress",
                        serde_json::json!({
                            "transferId": transfer.transfer_id.clone(),
                            "receivedBytes": sent,
                        }),
                    );
                }
                if sent == size && !transfer.completion_sent {
                    transfer.completion_sent = true;
                    let transfer_id = transfer.transfer_id.clone();
                    send_signal(socket, serde_json::json!({
                        "type": "file_complete",
                        "transfer_id": transfer_id,
                    }))
                    .await
                    .map_err(|error| format!("发送文件完成信号失败：{error}"))?;
                    let _ = state.app.emit(
                        "agent-file-transfer-status",
                        "数据已发送，等待控制端校验并保存。",
                    );
                }
            }
            finished = async {
                match agent_file_finish.as_mut() {
                    Some((_, _, _, task)) => task.await,
                    None => std::future::pending().await,
                }
            } => {
                let Some((transfer_id, file_name, received_size, _)) = agent_file_finish.take() else {
                    continue;
                };
                let saved = finished
                    .map_err(|error| format!("文件保存任务失败：{error}"))
                    .and_then(|result| result);
                let saved_path = match saved {
                    Ok(path) => path,
                    Err(error) => {
                        let _ = append_diagnostic_log(
                            &state.app,
                            DiagnosticEvent::FileTransferFailed,
                        );
                        send_signal(socket, serde_json::json!({
                            "type": "file_transfer_error",
                            "transfer_id": transfer_id,
                            "message": error,
                        }))
                        .await
                        .map_err(|error| format!("发送文件错误状态失败：{error}"))?;
                        continue;
                    }
                };
                let _ = append_diagnostic_log(
                    &state.app,
                    DiagnosticEvent::FileTransferCompleted,
                );
                send_signal(
                    socket,
                    serde_json::json!({
                        "type": "file_transfer_complete",
                        "transfer_id": transfer_id.clone(),
                    }),
                )
                .await
                .map_err(|error| format!("发送文件完成状态失败：{error}"))?;
                state
                    .app
                    .emit(
                        "remote-file-received",
                        serde_json::json!({
                            "transferId": transfer_id,
                            "fileName": file_name,
                            "savedPath": saved_path,
                            "controllerName": controller_name,
                            "receivedBytes": received_size,
                        }),
                    )
                    .map_err(|error| format!("无法显示文件保存状态：{error}"))?;
            }
            file_command = file_receiver.recv() => {
                match file_command {
                    Some(AgentFileCommand::Offer { transfer_id, source }) => {
                        if !is_valid_file_offer(&transfer_id, &source.file_name, source.size, &source.sha256) {
                            state.app.emit("agent-file-transfer-decision", serde_json::json!({
                                "transferId": transfer_id,
                                "accepted": false,
                                "error": "文件信息无效或超过传输限制。",
                            })).map_err(|error| format!("无法报告文件传输状态：{error}"))?;
                            continue;
                        }
                        if agent_file_transfer.is_some()
                            || agent_incoming_file_transfer.is_some()
                            || agent_file_finish.is_some()
                        {
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
                            "file_name": source.file_name,
                            "size": source.size,
                            "sha256": source.sha256,
                            "segment_hashes": source.segment_hashes,
                        }))
                        .await
                        .map_err(|error| format!("发送文件请求失败：{error}"))?;
                        agent_file_transfer = Some(AgentOutgoingFileTransfer::new(transfer_id, source));
                    }
                    Some(AgentFileCommand::Control { transfer_id, action }) => {
                        let is_outgoing = agent_file_transfer
                            .as_ref()
                            .is_some_and(|transfer| transfer.transfer_id == transfer_id);
                        let is_incoming = agent_incoming_file_transfer
                            .as_ref()
                            .is_some_and(|incoming| incoming.receiver.metadata.transfer_id == transfer_id);
                        if !is_outgoing && !is_incoming {
                            let message = "文件传输已完成或请求已过期。".to_string();
                            let _ = state.app.emit(
                                "agent-file-transfer-error",
                                serde_json::json!({
                                    "transferId": transfer_id.clone(),
                                    "message": message.clone(),
                                }),
                            );
                            send_signal(socket, serde_json::json!({
                                "type": "file_transfer_error",
                                "transfer_id": transfer_id.clone(),
                                "message": message,
                            }))
                            .await
                            .map_err(|error| format!("发送文件传输错误状态失败：{error}"))?;
                            continue;
                        }
                        send_signal(socket, serde_json::json!({
                            "type": "file_transfer_control",
                            "transfer_id": transfer_id,
                            "action": action,
                        }))
                        .await
                        .map_err(|error| format!("发送文件传输控制消息失败：{error}"))?;
                        if is_outgoing {
                            if action == "cancel" {
                                agent_file_transfer = None;
                            } else if let Some(transfer) = agent_file_transfer.as_mut() {
                                transfer.paused = action == "pause";
                            }
                        } else if is_incoming {
                            match action.as_str() {
                                "pause" => file_receive_deadline = None,
                                "resume" => {
                                    file_receive_deadline = Some(
                                        tokio::time::Instant::now() + Duration::from_secs(30),
                                    );
                                }
                                "cancel" => {
                                    if let Some(mut incoming) = agent_incoming_file_transfer.take() {
                                        discard_partial_file_receive(&mut incoming.receiver)?;
                                    }
                                    file_receive_deadline = None;
                                }
                                _ => unreachable!(),
                            }
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
            _ = async {
                match peer_disconnect_deadline {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => std::future::pending().await,
                }
            } => {
                return Err("WebRTC 连接中断，请检查局域网和防火墙 UDP 设置。".to_string());
            }
        }
    }

    drop(stats_worker);
    drop(audio_capture_worker);
    drop(capture_worker);
    drop(input_sender);
    drop(data_channels);
    drop(peer_connection);
    Ok(())
}

fn open_partial_file_receiver(
    app: &AppHandle,
    transfer_id: String,
    file_name: String,
    expected_size: u64,
    sha256: String,
    segment_hashes: Vec<String>,
) -> Result<ControllerFileReceiver, String> {
    let mut directory = app
        .path()
        .app_data_dir()
        .map_err(|error| format!("无法定位文件续传目录：{error}"))?;
    directory.push("partial-transfers");
    let mut receiver = open_partial_file_receiver_with_segments_in(
        &directory,
        transfer_id,
        file_name,
        expected_size,
        sha256,
        segment_hashes,
    )?;
    let download_dir = app.path().download_dir().ok();
    if let Err(error) = ensure_receive_disk_space(&receiver, &directory, download_dir.as_deref()) {
        if receiver.received_size == 0 {
            let _ = discard_partial_file_receive(&mut receiver);
        }
        return Err(error);
    }
    Ok(receiver)
}

/// 接受文件前检查磁盘空间：续传目录需容纳剩余数据；若下载目录在另一块磁盘上
/// （无法直接改名，需要复制），下载目录还需容纳整个文件。
fn ensure_receive_disk_space(
    receiver: &ControllerFileReceiver,
    partial_dir: &Path,
    download_dir: Option<&Path>,
) -> Result<(), String> {
    let remaining = receiver
        .metadata
        .expected_size
        .saturating_sub(receiver.received_size);
    ensure_disk_space(partial_dir, remaining.saturating_add(DISK_SPACE_MARGIN), "续传临时目录")?;
    if let Some(download_dir) = download_dir.filter(|path| path.exists())
        && !same_volume(partial_dir, download_dir)
    {
        ensure_disk_space(
            download_dir,
            receiver.metadata.expected_size.saturating_add(DISK_SPACE_MARGIN),
            "下载目录",
        )?;
    }
    Ok(())
}

fn ensure_disk_space(directory: &Path, required: u64, label: &str) -> Result<(), String> {
    let available = fs2::available_space(directory)
        .map_err(|error| format!("无法检查{label}的剩余空间：{error}"))?;
    if available < required {
        return Err(format!(
            "{label}剩余空间不足：需要约 {}，当前可用 {}。请清理磁盘后重试，已接收的进度会保留。",
            format_bytes(required),
            format_bytes(available),
        ));
    }
    Ok(())
}

fn format_bytes(bytes: u64) -> String {
    const GIB: f64 = 1024.0 * 1024.0 * 1024.0;
    const MIB: f64 = 1024.0 * 1024.0;
    if bytes as f64 >= GIB {
        format!("{:.2} GiB", bytes as f64 / GIB)
    } else {
        format!("{:.1} MiB", bytes as f64 / MIB)
    }
}

#[cfg(unix)]
fn same_volume(left: &Path, right: &Path) -> bool {
    use std::os::unix::fs::MetadataExt;
    match (fs::metadata(left), fs::metadata(right)) {
        (Ok(left), Ok(right)) => left.dev() == right.dev(),
        _ => false,
    }
}

#[cfg(windows)]
fn same_volume(left: &Path, right: &Path) -> bool {
    use std::path::Component;
    let prefix = |path: &Path| match path.components().next() {
        Some(Component::Prefix(prefix)) => {
            Some(prefix.as_os_str().to_string_lossy().to_ascii_lowercase())
        }
        _ => None,
    };
    matches!((prefix(left), prefix(right)), (Some(left), Some(right)) if left == right)
}

#[cfg(not(any(unix, windows)))]
fn same_volume(_left: &Path, _right: &Path) -> bool {
    false
}

/// 先写临时文件再改名，避免写记录时崩溃导致续传记录损坏。
fn write_partial_metadata(path: &Path, metadata: &PartialTransferMetadata) -> Result<(), String> {
    let contents =
        serde_json::to_vec(metadata).map_err(|error| format!("无法生成文件续传记录：{error}"))?;
    let temporary = path.with_extension("json.tmp");
    fs::write(&temporary, contents).map_err(|error| format!("无法保存文件续传记录：{error}"))?;
    fs::rename(&temporary, path).map_err(|error| format!("无法保存文件续传记录：{error}"))
}

#[cfg(test)]
fn open_partial_file_receiver_in(
    directory: &Path,
    transfer_id: String,
    file_name: String,
    expected_size: u64,
    sha256: String,
) -> Result<ControllerFileReceiver, String> {
    open_partial_file_receiver_with_segments_in(
        directory,
        transfer_id,
        file_name,
        expected_size,
        sha256,
        Vec::new(),
    )
}

fn open_partial_file_receiver_with_segments_in(
    directory: &Path,
    transfer_id: String,
    file_name: String,
    expected_size: u64,
    sha256: String,
    segment_hashes: Vec<String>,
) -> Result<ControllerFileReceiver, String> {
    if !is_valid_file_offer(&transfer_id, &file_name, expected_size, &sha256)
        || !is_valid_segment_hashes(expected_size, &segment_hashes)
    {
        return Err("文件续传信息无效。".to_string());
    }
    fs::create_dir_all(directory).map_err(|error| format!("无法创建文件续传目录：{error}"))?;
    cleanup_expired_partial_transfers(directory)?;

    let partial_path = directory.join(format!("{transfer_id}.part"));
    let metadata_path = directory.join(format!("{transfer_id}.json"));
    let mut metadata = match fs::read(&metadata_path) {
        Ok(contents) => {
            let mut metadata: PartialTransferMetadata = serde_json::from_slice(&contents)
                .map_err(|error| format!("文件续传记录损坏：{error}"))?;
            if metadata.transfer_id != transfer_id
                || metadata.file_name != file_name
                || metadata.expected_size != expected_size
                || metadata.sha256 != sha256
                || (!metadata.segment_hashes.is_empty()
                    && !segment_hashes.is_empty()
                    && metadata.segment_hashes != segment_hashes)
            {
                return Err("续传文件与现有未完成传输不匹配；请取消旧传输后重试。".to_string());
            }
            if metadata.segment_hashes.is_empty() {
                metadata.segment_hashes = segment_hashes;
            }
            metadata
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if partial_path.exists() {
                fs::remove_file(&partial_path)
                    .map_err(|error| format!("无法清理无效的续传数据：{error}"))?;
            }
            let metadata = PartialTransferMetadata {
                transfer_id,
                file_name,
                expected_size,
                sha256,
                updated_at: unix_timestamp()?,
                synced_size: Some(0),
                segment_hashes,
            };
            write_partial_metadata(&metadata_path, &metadata)?;
            metadata
        }
        Err(error) => return Err(format!("无法读取文件续传记录：{error}")),
    };
    let file_size = match fs::metadata(&partial_path) {
        Ok(metadata) => metadata.len(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
        Err(error) => return Err(format!("无法检查续传文件：{error}")),
    };
    if file_size > expected_size {
        return Err("续传文件大小超过原文件，已拒绝继续。".to_string());
    }
    // 安全回退：只信任最近一次 fsync 检查点之前的数据，丢弃崩溃时可能残留的脏尾巴，
    // 避免整份文件在最终 SHA-256 校验时失败而被删除。
    let received_size = match metadata.synced_size {
        Some(synced) => synced.min(file_size),
        None => file_size.saturating_sub(LEGACY_RESUME_ROLLBACK),
    };
    if received_size < file_size {
        truncate_partial_file(&partial_path, received_size)?;
    }
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(&partial_path)
        .map_err(|error| format!("无法打开续传文件：{error}"))?;
    metadata.synced_size = Some(received_size);
    metadata.updated_at = unix_timestamp()?;
    write_partial_metadata(&metadata_path, &metadata)?;
    Ok(ControllerFileReceiver {
        metadata,
        file: Some(file),
        partial_path,
        metadata_path,
        received_size,
    })
}

fn append_partial_file_chunk(
    receiver: &mut ControllerFileReceiver,
    bytes: &[u8],
) -> Result<(), String> {
    if bytes.is_empty() || bytes.len() > MAX_FILE_CHUNK_SIZE {
        return Err("文件数据块无效。".to_string());
    }
    let next_size = receiver.received_size.saturating_add(bytes.len() as u64);
    if next_size > receiver.metadata.expected_size {
        return Err("收到的文件数据超过声明大小。".to_string());
    }
    receiver
        .file
        .as_mut()
        .ok_or_else(|| "文件接收句柄不可用。".to_string())?
        .write_all(bytes)
        .map_err(|error| format!("写入接收文件失败：{error}"))?;
    receiver.received_size = next_size;

    let synced = receiver.metadata.synced_size.unwrap_or(0);
    let now = unix_timestamp()?;
    if next_size.saturating_sub(synced) >= PARTIAL_SYNC_CHECKPOINT {
        receiver
            .file
            .as_ref()
            .ok_or_else(|| "文件接收句柄不可用。".to_string())?
            .sync_data()
            .map_err(|error| format!("无法将接收数据写入磁盘：{error}"))?;
        receiver.metadata.synced_size = Some(next_size);
        receiver.metadata.updated_at = now;
        write_partial_metadata(&receiver.metadata_path, &receiver.metadata)?;
    } else if now.saturating_sub(receiver.metadata.updated_at) >= 60 {
        receiver.metadata.updated_at = now;
        write_partial_metadata(&receiver.metadata_path, &receiver.metadata)?;
    }
    Ok(())
}

fn cleanup_expired_partial_transfers(directory: &Path) -> Result<(), String> {
    let now = unix_timestamp()?;
    let expiry = PARTIAL_TRANSFER_TTL.as_secs();
    let entries =
        fs::read_dir(directory).map_err(|error| format!("无法检查未完成传输：{error}"))?;
    for entry in entries {
        let entry = entry.map_err(|error| format!("无法读取未完成传输目录：{error}"))?;
        if entry
            .path()
            .extension()
            .is_none_or(|extension| extension != "json")
        {
            continue;
        }
        let Ok(contents) = fs::read(entry.path()) else {
            continue;
        };
        let Ok(metadata) = serde_json::from_slice::<PartialTransferMetadata>(&contents) else {
            continue;
        };
        if !is_valid_transfer_id(&metadata.transfer_id)
            || entry.path().file_stem().and_then(|stem| stem.to_str())
                != Some(metadata.transfer_id.as_str())
        {
            continue;
        }
        if now.saturating_sub(metadata.updated_at) > expiry {
            let partial_path = directory.join(format!("{}.part", metadata.transfer_id));
            remove_file_if_present(&partial_path, "无法清理过期续传数据")?;
            remove_file_if_present(&entry.path(), "无法清理过期续传记录")?;
        }
    }
    Ok(())
}

fn finish_partial_file_receive(
    app: &AppHandle,
    receiver: ControllerFileReceiver,
) -> Result<String, String> {
    let mut directory = app
        .path()
        .download_dir()
        .map_err(|error| format!("无法定位下载目录：{error}"))?;
    directory.push("Lantern Received");
    finish_partial_file_receive_in(receiver, &directory)
}

fn finish_partial_file_receive_in(
    mut receiver: ControllerFileReceiver,
    directory: &Path,
) -> Result<String, String> {
    if receiver.received_size != receiver.metadata.expected_size {
        return Err("文件数据未完整，续传进度已保留。".to_string());
    }
    receiver
        .file
        .as_ref()
        .ok_or_else(|| "文件接收句柄不可用。".to_string())?
        .sync_all()
        .map_err(|error| format!("无法完成接收文件写入：{error}"))?;
    let digest = digest_file_with_progress(&receiver.partial_path, |_| {})?;
    if digest.sha256 != receiver.metadata.sha256 {
        if let Some(bad_segment) =
            first_mismatched_segment(&digest.segment_hashes, &receiver.metadata.segment_hashes)
        {
            // 只回退到第一个损坏段的开头，之前完好的数据保留，重新发送同一文件即可从这里续传。
            let keep = bad_segment as u64 * FILE_SEGMENT_SIZE;
            receiver.file.take();
            truncate_partial_file(&receiver.partial_path, keep)?;
            receiver.received_size = keep;
            receiver.metadata.synced_size = Some(keep);
            receiver.metadata.updated_at = unix_timestamp()?;
            write_partial_metadata(&receiver.metadata_path, &receiver.metadata)?;
            return Err(format!(
                "文件第 {} 段（共 {} 段）校验失败，已保留之前完好的 {}；重新发送同一文件即可从该处续传。",
                bad_segment + 1,
                receiver.metadata.segment_hashes.len(),
                format_bytes(keep),
            ));
        }
        discard_partial_file_receive(&mut receiver)?;
        return Err("文件校验失败，未保存损坏文件。".to_string());
    }
    receiver.file.take();
    let (destination, destination_path) =
        create_received_file_in(directory, &receiver.metadata.file_name)?;
    drop(destination);
    // 同一磁盘上直接改名（瞬间完成，不额外占用空间）；跨磁盘时退回复制。
    if fs::rename(&receiver.partial_path, &destination_path).is_err() {
        let copy_result = (|| {
            let mut source = File::open(&receiver.partial_path)
                .map_err(|error| format!("无法打开续传文件：{error}"))?;
            let mut destination = OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(&destination_path)
                .map_err(|error| format!("无法打开接收文件：{error}"))?;
            std::io::copy(&mut source, &mut destination)
                .map_err(|error| format!("无法保存接收文件：{error}"))?;
            destination
                .sync_all()
                .map_err(|error| format!("无法完成接收文件写入：{error}"))?;
            Ok::<(), String>(())
        })();
        if let Err(error) = copy_result {
            let _ = fs::remove_file(&destination_path);
            return Err(error);
        }
        fs::remove_file(&receiver.partial_path)
            .map_err(|error| format!("文件已保存，但无法清理续传数据：{error}"))?;
    }
    fs::remove_file(&receiver.metadata_path)
        .map_err(|error| format!("文件已保存，但无法清理续传记录：{error}"))?;
    Ok(destination_path.to_string_lossy().into_owned())
}

fn discard_partial_file_receive(receiver: &mut ControllerFileReceiver) -> Result<(), String> {
    receiver.file.take();
    remove_file_if_present(&receiver.partial_path, "无法清理未完成的接收文件")?;
    remove_file_if_present(&receiver.metadata_path, "无法清理文件续传记录")?;
    remove_file_if_present(
        &receiver.metadata_path.with_extension("json.tmp"),
        "无法清理文件续传临时记录",
    )
}

fn remove_file_if_present(path: &Path, message: &str) -> Result<(), String> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!("{message}：{error}")),
    }
}

#[cfg(test)]
fn hash_file(path: &Path) -> Result<String, String> {
    hash_file_with_progress(path, |_| {})
}

#[cfg(test)]
fn hash_file_with_progress(path: &Path, progress: impl FnMut(u64)) -> Result<String, String> {
    digest_file_with_progress(path, progress).map(|digest| digest.sha256)
}

/// 一次读盘同时算出整文件 SHA-256 和每 64 MiB 分段的 SHA-256，
/// 内存占用固定为一个缓冲区，与文件大小无关。
fn digest_file_with_progress(
    path: &Path,
    mut progress: impl FnMut(u64),
) -> Result<FileDigest, String> {
    let mut file = File::open(path).map_err(|error| format!("无法打开文件进行校验：{error}"))?;
    let mut hasher = Sha256::new();
    let mut segment_hasher = Sha256::new();
    let mut segment_filled = 0_u64;
    let mut segment_hashes = Vec::new();
    let mut buffer = vec![0_u8; MAX_FILE_CHUNK_SIZE];
    let mut hashed = 0_u64;
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| format!("读取文件以进行校验失败：{error}"))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        let mut remaining = &buffer[..read];
        while !remaining.is_empty() {
            let take = remaining
                .len()
                .min((FILE_SEGMENT_SIZE - segment_filled) as usize);
            segment_hasher.update(&remaining[..take]);
            segment_filled += take as u64;
            remaining = &remaining[take..];
            if segment_filled == FILE_SEGMENT_SIZE {
                segment_hashes.push(format!("{:x}", segment_hasher.finalize_reset()));
                segment_filled = 0;
            }
        }
        let previous = hashed;
        hashed += read as u64;
        if hashed / FILE_HASH_PROGRESS_STEP > previous / FILE_HASH_PROGRESS_STEP {
            progress(hashed);
        }
    }
    if segment_filled > 0 {
        segment_hashes.push(format!("{:x}", segment_hasher.finalize()));
    }
    Ok(FileDigest {
        sha256: format!("{:x}", hasher.finalize()),
        segment_hashes,
    })
}

/// 返回第一个与发送端分段哈希不一致的段号；没有分段信息时返回 None（只能整体重传）。
fn first_mismatched_segment(actual: &[String], expected: &[String]) -> Option<usize> {
    if expected.is_empty() || actual.len() != expected.len() {
        return None;
    }
    actual
        .iter()
        .zip(expected)
        .position(|(actual, expected)| actual != expected)
}

/// 用单独的可写句柄截断（Windows 上以追加方式打开的句柄不能修改文件长度）。
fn truncate_partial_file(path: &Path, length: u64) -> Result<(), String> {
    let file = OpenOptions::new()
        .write(true)
        .open(path)
        .map_err(|error| format!("无法回退续传数据到安全位置：{error}"))?;
    file.set_len(length)
        .map_err(|error| format!("无法回退续传数据到安全位置：{error}"))?;
    file.sync_data()
        .map_err(|error| format!("无法回退续传数据到安全位置：{error}"))
}

fn take_prepared_outgoing_file(
    remote: &RemoteState,
    source_id: &str,
) -> Result<Arc<OutgoingFileSource>, String> {
    let mut files = remote
        .outgoing_files
        .lock()
        .map_err(|_| "文件发送状态不可用".to_string())?;
    let index = files
        .iter()
        .position(|(id, _)| id.as_str() == source_id)
        .ok_or_else(|| "所选文件已失效，请重新选择。".to_string())?;
    Ok(files.remove(index).1)
}

fn prepared_outgoing_file(
    remote: &RemoteState,
    source_id: &str,
) -> Result<Arc<OutgoingFileSource>, String> {
    remote
        .outgoing_files
        .lock()
        .map_err(|_| "文件发送状态不可用".to_string())?
        .iter()
        .find(|(id, _)| id.as_str() == source_id)
        .map(|(_, source)| Arc::clone(source))
        .ok_or_else(|| "所选文件已失效，请重新选择。".to_string())
}

fn prepare_outgoing_file_source(
    remote: &RemoteState,
    path: PathBuf,
    on_progress: &tauri::ipc::Channel<serde_json::Value>,
) -> Result<OutgoingFileSource, String> {
    let metadata = fs::metadata(&path).map_err(|error| format!("无法读取所选文件：{error}"))?;
    if !metadata.is_file() {
        return Err("只能发送普通文件。".to_string());
    }
    let size = metadata.len();
    if size == 0 || size > MAX_FILE_TRANSFER_SIZE {
        return Err("文件必须大于 0 且不超过 100 GiB。".to_string());
    }
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .map(str::to_string)
        .ok_or_else(|| "文件名不是有效的 UTF-8。".to_string())?;
    if file_name.len() > 255 || file_name.chars().any(|character| character.is_control()) {
        return Err("文件名过长或包含无效字符。".to_string());
    }
    let modified = metadata.modified().ok();
    let cached = {
        let mut cache = remote
            .file_hash_cache
            .lock()
            .map_err(|_| "文件校验缓存不可用".to_string())?;
        match cache.iter().position(|entry| entry.path == path) {
            Some(index) if cache[index].size == size && cache[index].modified == modified => {
                // 命中后移到末尾，淘汰时优先移除最久未用的条目。
                let entry = cache.remove(index);
                let digest = entry.digest.clone();
                cache.push(entry);
                Some(digest)
            }
            Some(index) => {
                cache.remove(index);
                None
            }
            None => None,
        }
    };
    let digest = match cached {
        Some(digest) => digest,
        None => {
            let _ = on_progress.send(serde_json::json!({
                "fileName": file_name,
                "hashedBytes": 0,
                "size": size,
            }));
            let digest = digest_file_with_progress(&path, |hashed| {
                let _ = on_progress.send(serde_json::json!({
                    "fileName": file_name,
                    "hashedBytes": hashed,
                    "size": size,
                }));
            })?;
            let mut cache = remote
                .file_hash_cache
                .lock()
                .map_err(|_| "文件校验缓存不可用".to_string())?;
            cache.retain(|entry| entry.path != path);
            if cache.len() >= MAX_FILE_HASH_CACHE_ENTRIES {
                cache.remove(0);
            }
            cache.push(CachedFileDigest {
                path: path.clone(),
                size,
                modified,
                digest: digest.clone(),
            });
            digest
        }
    };
    let after = fs::metadata(&path).map_err(|error| format!("无法读取所选文件：{error}"))?;
    if after.len() != size || after.modified().ok() != modified {
        return Err("文件在校验期间被修改，请稍后重新选择。".to_string());
    }
    Ok(OutgoingFileSource {
        path,
        file_name,
        size,
        modified,
        sha256: digest.sha256,
        segment_hashes: digest.segment_hashes,
    })
}

fn open_outgoing_file_source(source: &OutgoingFileSource, offset: u64) -> Result<File, String> {
    if offset > source.size {
        return Err("续传进度超过文件长度。".to_string());
    }
    let mut file =
        File::open(&source.path).map_err(|error| format!("无法打开要发送的文件：{error}"))?;
    let metadata = file
        .metadata()
        .map_err(|error| format!("无法读取要发送的文件：{error}"))?;
    if metadata.len() != source.size || metadata.modified().ok() != source.modified {
        return Err("文件在选择后已被修改，请重新选择。".to_string());
    }
    file.seek(SeekFrom::Start(offset))
        .map_err(|error| format!("无法定位文件续传位置：{error}"))?;
    Ok(file)
}

fn read_outgoing_chunk(
    file: &mut File,
    source: &OutgoingFileSource,
    offset: u64,
) -> Result<Vec<u8>, String> {
    let remaining = source.size.saturating_sub(offset);
    if remaining == 0 {
        return Err("文件数据已全部发送。".to_string());
    }
    let length = remaining.min(MAX_FILE_CHUNK_SIZE as u64) as usize;
    let mut buffer = vec![0_u8; length];
    file.read_exact(&mut buffer)
        .map_err(|error| format!("读取文件数据失败（文件可能已被修改）：{error}"))?;
    Ok(buffer)
}

fn unix_timestamp() -> Result<u64, String> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|error| format!("无法读取系统时间：{error}"))
}

fn create_received_file_in(directory: &Path, file_name: &str) -> Result<(File, PathBuf), String> {
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

fn capture_dimensions(display_id: Option<u32>) -> Result<(u32, u32, i32, i32), String> {
    let monitor = selected_monitor(display_id)?;
    Ok((monitor.width, monitor.height, monitor.x, monitor.y))
}

fn capture_bgra(max_width: u32, display_id: Option<u32>) -> Result<(Vec<u8>, u32, u32), String> {
    let monitor = selected_monitor(display_id)?;
    let pixels = monitor
        .monitor
        .capture_image()
        .map_err(|error| format!("屏幕采集失败：{error}"))?;
    let width = pixels.width();
    let height = pixels.height();
    if width == 0 || height == 0 {
        return Err("屏幕图像格式无效".to_string());
    }

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

/// 一台可采集的显示器。坐标和尺寸与原 screenshots 库口径一致：
/// macOS 为逻辑点（CGDisplayBounds），Windows 为物理像素，Linux 为按缩放换算后的值，
/// 供键鼠坐标映射使用。
struct CaptureMonitor {
    id: u32,
    x: i32,
    y: i32,
    width: u32,
    height: u32,
    primary: bool,
    monitor: xcap::Monitor,
}

fn capture_monitors() -> Result<Vec<CaptureMonitor>, String> {
    let monitors = xcap::Monitor::all().map_err(|error| error.to_string())?;
    let mut result = Vec::with_capacity(monitors.len());
    for monitor in monitors {
        let describe = |error: xcap::XCapError| format!("无法读取显示器信息：{error}");
        result.push(CaptureMonitor {
            id: stable_monitor_id(&monitor).map_err(describe)?,
            x: monitor.x().map_err(describe)?,
            y: monitor.y().map_err(describe)?,
            width: monitor.width().map_err(describe)?,
            height: monitor.height().map_err(describe)?,
            primary: monitor.is_primary().map_err(describe)?,
            monitor,
        });
    }
    Ok(result)
}

/// Windows 上 xcap 的 id 是 HMONITOR 句柄，重启或插拔显示器后会变。
/// 这里沿用原 screenshots 库的算法（设备名的 fxhash），让已保存的显示器选择继续有效。
#[cfg(target_os = "windows")]
fn stable_monitor_id(monitor: &xcap::Monitor) -> xcap::XCapResult<u32> {
    Ok(fxhash::hash32(monitor.name()?.as_bytes()))
}

/// macOS 为 CGDirectDisplayID，Linux 为 RandR output id，与原 screenshots 库相同。
#[cfg(not(target_os = "windows"))]
fn stable_monitor_id(monitor: &xcap::Monitor) -> xcap::XCapResult<u32> {
    monitor.id()
}

fn selected_monitor(display_id: Option<u32>) -> Result<CaptureMonitor, String> {
    let monitors = capture_monitors().map_err(|error| format!("无法读取屏幕：{error}"))?;
    if let Some(display_id) = display_id {
        return monitors
            .into_iter()
            .find(|monitor| monitor.id == display_id)
            .ok_or_else(|| "所选显示器已不可用，请在应用设置中重新选择。".to_string());
    }
    let primary = monitors.iter().position(|monitor| monitor.primary).unwrap_or(0);
    monitors
        .into_iter()
        .nth(primary)
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
    origin_x: i32,
    origin_y: i32,
    state: &mut InputState,
) -> Result<(), String> {
    match message {
        ClientMessage::MouseMove { x, y } if x.is_finite() && y.is_finite() => {
            let x = origin_x + (x.clamp(0.0, 1.0) * width.saturating_sub(1) as f64).round() as i32;
            let y = origin_y + (y.clamp(0.0, 1.0) * height.saturating_sub(1) as f64).round() as i32;
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
        | ClientMessage::FileTransferError { .. }
        | ClientMessage::FileComplete { .. }
        | ClientMessage::FileTransferComplete { .. }
        | ClientMessage::FileTransferControl { .. }
        | ClientMessage::FileTransferDecision { .. }
        | ClientMessage::FileAck { .. }
        | ClientMessage::QualityAdjust { .. } => {}
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
    fn constrained_network_bitrate_is_lower_and_normal_restores_profile() {
        let settings = VideoQuality::Quality.settings();
        let normal = bitrate_for_network(settings, AdaptiveQuality::Normal);
        let constrained = bitrate_for_network(settings, AdaptiveQuality::Constrained);
        assert_eq!(normal, (settings.min_bitrate, settings.max_bitrate));
        assert!(constrained.0 >= 1_000_000);
        assert!(constrained.0 < normal.0);
        assert!(constrained.1 < normal.1);
        assert!(constrained.0 < constrained.1);
    }

    #[test]
    fn system_audio_conversion_clamps_normalized_samples_to_pcm16() {
        assert_eq!(
            system_audio_to_pcm(&[-1.5, -1.0, -0.5, 0.0, 0.5, 1.0, 1.5]),
            vec![-32767, -32767, -16384, 0, 16384, 32767, 32767]
        );
    }

    #[test]
    fn discovery_uses_each_lan_subnet_broadcast_address() {
        assert_eq!(
            subnet_broadcast(
                Ipv4Addr::new(192, 168, 1, 23),
                Ipv4Addr::new(255, 255, 255, 0)
            ),
            Some(Ipv4Addr::new(192, 168, 1, 255))
        );
        assert_eq!(
            subnet_broadcast(
                Ipv4Addr::new(192, 168, 1, 23),
                Ipv4Addr::new(255, 255, 255, 255)
            ),
            None
        );
    }

    #[test]
    fn local_device_detection_matches_interface_and_loopback_addresses() {
        let local_addresses = HashSet::from([
            IpAddr::V4(Ipv4Addr::new(192, 168, 1, 23)),
            IpAddr::V4(Ipv4Addr::LOCALHOST),
        ]);
        assert!(is_local_device_ip(
            IpAddr::V4(Ipv4Addr::new(192, 168, 1, 23)),
            &local_addresses
        ));
        assert!(is_local_device_ip(
            IpAddr::V6("::ffff:192.168.1.23".parse().unwrap()),
            &local_addresses
        ));
        assert!(!is_local_device_ip(
            IpAddr::V4(Ipv4Addr::new(192, 168, 1, 24)),
            &local_addresses
        ));
    }

    #[test]
    fn answering_recvonly_transceivers_publishes_system_audio_track() {
        let factory = PeerConnectionFactory::builder().build().unwrap();
        let offerer = factory
            .create_peer_connection(&RtcConfiguration::default(), PeerConnectionObserver::new())
            .unwrap();
        offerer
            .add_transceiver(MediaKind::Video, TransceiverDirection::RecvOnly)
            .unwrap();
        offerer
            .add_transceiver(MediaKind::Audio, TransceiverDirection::RecvOnly)
            .unwrap();
        let offer = offerer.create_offer().unwrap();
        offerer.set_local_description(&offer).unwrap();

        let answerer = factory
            .create_peer_connection(&RtcConfiguration::default(), PeerConnectionObserver::new())
            .unwrap();
        answerer.set_remote_description(&offer).unwrap();
        let video = factory.create_video_track("test-screen").unwrap();
        let mut options = AudioTrackOptions::default();
        options.source = AudioTrackSource::LocalPush;
        let audio = factory
            .create_audio_track_with_options("test-system-audio", options)
            .unwrap();
        for transceiver in answerer.transceivers() {
            match transceiver.kind() {
                MediaKind::Video => {
                    transceiver.set_track(&video).unwrap();
                    transceiver
                        .set_direction(TransceiverDirection::SendOnly)
                        .unwrap();
                }
                MediaKind::Audio => {
                    transceiver.set_track(&audio).unwrap();
                    transceiver
                        .set_direction(TransceiverDirection::SendOnly)
                        .unwrap();
                }
                MediaKind::Unknown => {}
            }
        }
        let answer = answerer.create_answer().unwrap();
        let mut in_audio_section = false;
        let audio_is_sendonly = answer.sdp.lines().any(|line| {
            if line.starts_with("m=") {
                in_audio_section = line.starts_with("m=audio ");
            }
            in_audio_section && line.trim() == "a=sendonly"
        });
        assert!(
            audio_is_sendonly,
            "answer did not publish audio:\n{}",
            answer.sdp
        );
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
                system_audio_enabled: false,
                ..
            }
        ));
        assert!(matches!(
            serde_json::from_str::<ClientMessage>(
                r#"{"type":"hello","device_name":"Lantern","system_audio_enabled":true}"#
            ),
            Ok(ClientMessage::Hello {
                system_audio_enabled: true,
                ..
            })
        ));
    }

    #[test]
    fn clipboard_and_file_messages_decode_with_expected_fields() {
        let hash = format!("{:x}", Sha256::digest(b"hello world"));
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
                &format!(
                    r#"{{"type":"file_offer","transfer_id":"transfer-1","file_name":"notes.txt","size":12,"sha256":"{hash}"}}"#
                )
            ),
            Ok(ClientMessage::FileOffer {
                transfer_id,
                file_name,
                size: 12,
                sha256,
                segment_hashes,
            }) if transfer_id == "transfer-1" && file_name == "notes.txt"
                && sha256 == hash && segment_hashes.is_empty()
        ));
        assert!(matches!(
            serde_json::from_str::<ClientMessage>(
                r#"{"type":"file_complete","transfer_id":"transfer-1"}"#
            ),
            Ok(ClientMessage::FileComplete { transfer_id }) if transfer_id == "transfer-1"
        ));
        assert!(matches!(
            serde_json::from_str::<ClientMessage>(
                r#"{"type":"file_transfer_decision","transfer_id":"transfer-1","accepted":true,"resume_offset":8}"#
            ),
            Ok(ClientMessage::FileTransferDecision {
                transfer_id,
                accepted: true,
                resume_offset: 8,
            })
                if transfer_id == "transfer-1"
        ));
        assert!(matches!(
            serde_json::from_str::<ClientMessage>(
                r#"{"type":"file_transfer_complete","transfer_id":"transfer-1"}"#
            ),
            Ok(ClientMessage::FileTransferComplete { transfer_id }) if transfer_id == "transfer-1"
        ));
        assert!(matches!(
            serde_json::from_str::<ClientMessage>(
                r#"{"type":"file_transfer_control","transfer_id":"transfer-1","action":"pause"}"#
            ),
            Ok(ClientMessage::FileTransferControl { transfer_id, action })
                if transfer_id == "transfer-1" && action == "pause"
        ));
    }

    #[test]
    fn file_ack_messages_decode_with_received_offset() {
        assert!(matches!(
            serde_json::from_str::<ClientMessage>(
                r#"{"type":"file_ack","transfer_id":"transfer-1","received":1048576}"#
            ),
            Ok(ClientMessage::FileAck { transfer_id, received: 1_048_576 })
                if transfer_id == "transfer-1"
        ));
    }

    #[test]
    fn hundred_gibibyte_files_are_within_the_transfer_limit() {
        let hash = format!("{:x}", Sha256::digest(b"large"));
        let size = 100 * 1024 * 1024 * 1024;
        assert!(is_valid_file_offer("transfer-1", "disk.img", size, &hash));
        // 100 GiB 的分段哈希列表（1600 段）放进文件请求后仍远小于单条消息上限。
        let segments = vec!["a".repeat(64); size.div_ceil(FILE_SEGMENT_SIZE) as usize];
        assert!(is_valid_segment_hashes(size, &segments));
        let offer = serde_json::json!({
            "type": "file_offer",
            "transfer_id": "transfer-1",
            "file_name": "disk.img",
            "size": size,
            "sha256": hash,
            "segment_hashes": segments,
        })
        .to_string();
        assert!(offer.len() < MAX_SIGNAL_MESSAGE_SIZE);
    }

    #[test]
    fn outgoing_file_sources_stream_in_bounded_chunks_and_detect_changes() {
        let directory =
            std::env::temp_dir().join(format!("lantern-outgoing-test-{}", Uuid::new_v4()));
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join("payload.bin");
        let contents: Vec<u8> = (0..MAX_FILE_CHUNK_SIZE + 10)
            .map(|index| (index % 251) as u8)
            .collect();
        fs::write(&path, &contents).unwrap();
        let metadata = fs::metadata(&path).unwrap();
        let mut progress_calls = 0;
        let sha256 = hash_file_with_progress(&path, |_| progress_calls += 1).unwrap();
        assert_eq!(progress_calls, 0);
        assert_eq!(sha256, format!("{:x}", Sha256::digest(&contents)));
        let source = Arc::new(OutgoingFileSource {
            path: path.clone(),
            file_name: "payload.bin".to_string(),
            size: metadata.len(),
            modified: metadata.modified().ok(),
            sha256,
            segment_hashes: Vec::new(),
        });

        let mut transfer = AgentOutgoingFileTransfer::new("transfer-1".to_string(), Arc::clone(&source));
        assert!(!transfer.can_send());
        transfer.start(0).unwrap();
        assert!(transfer.can_send());
        let first = transfer.read_next_chunk().unwrap();
        assert_eq!(first.len(), MAX_FILE_CHUNK_SIZE);
        transfer.sent += first.len() as u64;
        let second = transfer.read_next_chunk().unwrap();
        assert_eq!(second, contents[MAX_FILE_CHUNK_SIZE..]);
        transfer.sent += second.len() as u64;
        assert!(!transfer.can_send());

        let mut resumed = open_outgoing_file_source(&source, MAX_FILE_CHUNK_SIZE as u64).unwrap();
        assert_eq!(
            read_outgoing_chunk(&mut resumed, &source, MAX_FILE_CHUNK_SIZE as u64).unwrap(),
            contents[MAX_FILE_CHUNK_SIZE..]
        );
        assert!(open_outgoing_file_source(&source, source.size + 1).is_err());

        let mut window_limited =
            AgentOutgoingFileTransfer::new("transfer-2".to_string(), Arc::clone(&source));
        window_limited.start(0).unwrap();
        window_limited.sent = FILE_SEND_WINDOW.min(source.size - 1);
        window_limited.acknowledged = 0;
        assert_eq!(window_limited.can_send(), window_limited.sent < FILE_SEND_WINDOW);

        fs::write(&path, b"changed").unwrap();
        assert!(open_outgoing_file_source(&source, 0).is_err());
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn file_offer_validation_rejects_invalid_sizes_and_nonportable_paths() {
        let hash = format!("{:x}", Sha256::digest(b"notes"));
        assert!(is_valid_file_offer("transfer-1", "notes.txt", 12, &hash));
        assert!(!is_valid_file_offer("transfer-1", "notes.txt", 0, &hash));
        assert!(!is_valid_file_offer(
            "transfer-1",
            "notes.txt",
            MAX_FILE_TRANSFER_SIZE + 1,
            &hash,
        ));
        assert!(!is_valid_file_offer(
            "transfer-1",
            "../notes.txt",
            12,
            &hash
        ));
        assert!(!is_valid_file_offer(
            "transfer-1",
            "folder\\notes.txt",
            12,
            &hash
        ));
        assert!(!is_valid_file_offer("transfer-1", "notes\n.txt", 12, &hash));
        assert!(!is_valid_file_offer(
            &"x".repeat(65),
            "notes.txt",
            12,
            &hash
        ));
        assert!(!is_valid_file_offer(
            "transfer-1",
            &"x".repeat(256),
            12,
            &hash
        ));
        assert!(!is_valid_file_offer("../transfer", "notes.txt", 12, &hash));
        assert!(!is_valid_file_offer(
            "transfer-1",
            "notes.txt",
            12,
            "invalid"
        ));
    }

    #[test]
    fn incomplete_file_receivers_preserve_and_resume_partial_data() {
        let directory =
            std::env::temp_dir().join(format!("lantern-transfer-test-{}", Uuid::new_v4()));
        let bytes = b"resumable content";
        let hash = format!("{:x}", Sha256::digest(bytes));
        let mut receiver = open_partial_file_receiver_in(
            &directory,
            "transfer-1".to_string(),
            "partial.bin".to_string(),
            bytes.len() as u64,
            hash.clone(),
        )
        .unwrap();
        let partial_path = receiver.partial_path.clone();
        receiver.metadata.updated_at = 0;
        fs::write(
            &receiver.metadata_path,
            serde_json::to_vec(&receiver.metadata).unwrap(),
        )
        .unwrap();
        append_partial_file_chunk(&mut receiver, &bytes[..7]).unwrap();
        let persisted_metadata: PartialTransferMetadata =
            serde_json::from_slice(&fs::read(&receiver.metadata_path).unwrap()).unwrap();
        assert!(persisted_metadata.updated_at > 0);
        drop(receiver);
        assert!(partial_path.exists());

        let receiver = open_partial_file_receiver_in(
            &directory,
            "transfer-1".to_string(),
            "partial.bin".to_string(),
            bytes.len() as u64,
            hash.clone(),
        )
        .unwrap();
        assert_eq!(receiver.received_size, 7);
        assert!(
            open_partial_file_receiver_in(
                &directory,
                "transfer-1".to_string(),
                "partial.bin".to_string(),
                bytes.len() as u64,
                format!("{:x}", Sha256::digest(b"different content")),
            )
            .is_err()
        );
        let mut receiver = receiver;
        append_partial_file_chunk(&mut receiver, &bytes[7..]).unwrap();
        drop(receiver);
        assert_eq!(hash_file(&partial_path).unwrap(), hash);
        let mut receiver = open_partial_file_receiver_in(
            &directory,
            "transfer-1".to_string(),
            "partial.bin".to_string(),
            bytes.len() as u64,
            hash,
        )
        .unwrap();
        discard_partial_file_receive(&mut receiver).unwrap();
        assert!(!partial_path.exists());
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn resumed_transfers_roll_back_to_the_last_synced_checkpoint() {
        let directory =
            std::env::temp_dir().join(format!("lantern-transfer-rollback-{}", Uuid::new_v4()));
        let bytes = vec![7_u8; 32];
        let hash = format!("{:x}", Sha256::digest(&bytes));
        let receiver = open_partial_file_receiver_in(
            &directory,
            "transfer-1".to_string(),
            "rollback.bin".to_string(),
            bytes.len() as u64,
            hash.clone(),
        )
        .unwrap();
        let partial_path = receiver.partial_path.clone();
        let metadata_path = receiver.metadata_path.clone();
        drop(receiver);

        // 模拟崩溃：磁盘上有 20 字节，但最近检查点只确认了 12 字节。
        fs::write(&partial_path, &bytes[..20]).unwrap();
        let mut metadata: PartialTransferMetadata =
            serde_json::from_slice(&fs::read(&metadata_path).unwrap()).unwrap();
        metadata.synced_size = Some(12);
        write_partial_metadata(&metadata_path, &metadata).unwrap();
        let receiver = open_partial_file_receiver_in(
            &directory,
            "transfer-1".to_string(),
            "rollback.bin".to_string(),
            bytes.len() as u64,
            hash.clone(),
        )
        .unwrap();
        assert_eq!(receiver.received_size, 12);
        assert_eq!(fs::metadata(&partial_path).unwrap().len(), 12);
        drop(receiver);

        // 旧版记录没有检查点：保守回退 LEGACY_RESUME_ROLLBACK（小文件即回退到 0）。
        fs::write(&partial_path, &bytes[..20]).unwrap();
        let mut metadata: PartialTransferMetadata =
            serde_json::from_slice(&fs::read(&metadata_path).unwrap()).unwrap();
        metadata.synced_size = None;
        write_partial_metadata(&metadata_path, &metadata).unwrap();
        let mut receiver = open_partial_file_receiver_in(
            &directory,
            "transfer-1".to_string(),
            "rollback.bin".to_string(),
            bytes.len() as u64,
            hash,
        )
        .unwrap();
        assert_eq!(receiver.received_size, 0);
        discard_partial_file_receive(&mut receiver).unwrap();
        assert!(!partial_path.exists());
        assert!(!metadata_path.exists());
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn segment_hash_lists_are_validated_against_file_size() {
        let hash = "a".repeat(64);
        assert!(is_valid_segment_hashes(10, &[]));
        assert!(is_valid_segment_hashes(10, &[hash.clone()]));
        assert!(is_valid_segment_hashes(FILE_SEGMENT_SIZE, &[hash.clone()]));
        assert!(!is_valid_segment_hashes(FILE_SEGMENT_SIZE + 1, &[hash.clone()]));
        assert!(is_valid_segment_hashes(
            FILE_SEGMENT_SIZE + 1,
            &[hash.clone(), hash.clone()]
        ));
        assert!(!is_valid_segment_hashes(10, &["xyz".to_string()]));
        assert_eq!(
            first_mismatched_segment(
                &["a".to_string(), "b".to_string(), "c".to_string()],
                &["a".to_string(), "x".to_string(), "c".to_string()],
            ),
            Some(1)
        );
        assert_eq!(first_mismatched_segment(&["a".to_string()], &[]), None);
        assert_eq!(
            first_mismatched_segment(&["a".to_string()], &["a".to_string(), "b".to_string()]),
            None
        );
    }

    #[test]
    fn corrupt_segment_rolls_back_to_the_segment_start_instead_of_discarding() {
        let root =
            std::env::temp_dir().join(format!("lantern-transfer-segment-{}", Uuid::new_v4()));
        let partial_dir = root.join("partial");
        let downloads_dir = root.join("downloads");
        fs::create_dir_all(&root).unwrap();
        let contents: Vec<u8> = (0..FILE_SEGMENT_SIZE as usize + 10)
            .map(|index| (index % 253) as u8)
            .collect();
        let source_path = root.join("source.bin");
        fs::write(&source_path, &contents).unwrap();
        let digest = digest_file_with_progress(&source_path, |_| {}).unwrap();
        assert_eq!(digest.sha256, format!("{:x}", Sha256::digest(&contents)));
        assert_eq!(digest.segment_hashes.len(), 2);
        assert_eq!(
            digest.segment_hashes[1],
            format!("{:x}", Sha256::digest(&contents[FILE_SEGMENT_SIZE as usize..]))
        );

        let mut receiver = open_partial_file_receiver_with_segments_in(
            &partial_dir,
            "transfer-1".to_string(),
            "segmented.bin".to_string(),
            contents.len() as u64,
            digest.sha256.clone(),
            digest.segment_hashes.clone(),
        )
        .unwrap();
        let mut corrupted = contents.clone();
        *corrupted.last_mut().unwrap() ^= 0xff;
        receiver.file.as_mut().unwrap().write_all(&corrupted).unwrap();
        receiver.received_size = corrupted.len() as u64;
        let partial_path = receiver.partial_path.clone();
        let error = finish_partial_file_receive_in(receiver, &downloads_dir).unwrap_err();
        assert!(error.contains("第 2 段"));
        assert_eq!(fs::metadata(&partial_path).unwrap().len(), FILE_SEGMENT_SIZE);
        assert!(!downloads_dir.join("segmented.bin").exists());

        let receiver = open_partial_file_receiver_with_segments_in(
            &partial_dir,
            "transfer-1".to_string(),
            "segmented.bin".to_string(),
            contents.len() as u64,
            digest.sha256,
            digest.segment_hashes,
        )
        .unwrap();
        assert_eq!(receiver.received_size, FILE_SEGMENT_SIZE);
        drop(receiver);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn receiving_requires_enough_free_disk_space() {
        let directory =
            std::env::temp_dir().join(format!("lantern-transfer-space-{}", Uuid::new_v4()));
        let hash = format!("{:x}", Sha256::digest(b"space"));
        let mut receiver = open_partial_file_receiver_in(
            &directory,
            "transfer-1".to_string(),
            "space.bin".to_string(),
            1024,
            hash,
        )
        .unwrap();
        assert!(ensure_receive_disk_space(&receiver, &directory, None).is_ok());
        assert!(ensure_disk_space(&directory, u64::MAX, "测试目录").is_err());
        assert!(same_volume(&directory, &directory));
        discard_partial_file_receive(&mut receiver).unwrap();
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn expired_partial_transfers_are_removed() {
        let directory =
            std::env::temp_dir().join(format!("lantern-transfer-expiry-{}", Uuid::new_v4()));
        fs::create_dir_all(&directory).unwrap();
        let partial_path = directory.join("expired.part");
        fs::write(&partial_path, b"old data").unwrap();
        let metadata_path = directory.join("expired.json");
        fs::write(
            &metadata_path,
            serde_json::to_vec(&PartialTransferMetadata {
                transfer_id: "expired".to_string(),
                file_name: "old.bin".to_string(),
                expected_size: 8,
                sha256: format!("{:x}", Sha256::digest(b"old data")),
                updated_at: 0,
                synced_size: None,
                segment_hashes: Vec::new(),
            })
            .unwrap(),
        )
        .unwrap();
        cleanup_expired_partial_transfers(&directory).unwrap();
        assert!(!partial_path.exists());
        assert!(!metadata_path.exists());
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn completed_resumable_transfers_are_verified_and_saved_without_overwrite() {
        let root = std::env::temp_dir().join(format!("lantern-transfer-save-{}", Uuid::new_v4()));
        let partial_dir = root.join("partial");
        let downloads_dir = root.join("downloads");
        let contents = b"verified transfer content";
        let hash = format!("{:x}", Sha256::digest(contents));
        fs::create_dir_all(&partial_dir).unwrap();

        let mut receiver = open_partial_file_receiver_in(
            &partial_dir,
            "transfer-1".to_string(),
            "notes.txt".to_string(),
            contents.len() as u64,
            hash.clone(),
        )
        .unwrap();
        receiver.file.as_mut().unwrap().write_all(contents).unwrap();
        receiver.received_size = contents.len() as u64;
        let partial_path = receiver.partial_path.clone();
        let metadata_path = receiver.metadata_path.clone();
        let saved_path = finish_partial_file_receive_in(receiver, &downloads_dir).unwrap();
        assert_eq!(fs::read(&saved_path).unwrap(), contents);
        assert!(!partial_path.exists());
        assert!(!metadata_path.exists());

        let mut receiver = open_partial_file_receiver_in(
            &partial_dir,
            "transfer-1".to_string(),
            "notes.txt".to_string(),
            contents.len() as u64,
            hash,
        )
        .unwrap();
        receiver.file.as_mut().unwrap().write_all(contents).unwrap();
        receiver.received_size = contents.len() as u64;
        let second_path = finish_partial_file_receive_in(receiver, &downloads_dir).unwrap();
        assert_ne!(saved_path, second_path);
        assert_eq!(fs::read(&saved_path).unwrap(), contents);
        assert_eq!(fs::read(&second_path).unwrap(), contents);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn corrupt_resumed_transfer_is_removed_instead_of_saved() {
        let root = std::env::temp_dir().join(format!("lantern-transfer-hash-{}", Uuid::new_v4()));
        let partial_dir = root.join("partial");
        let downloads_dir = root.join("downloads");
        let expected = b"expected bytes";
        let hash = format!("{:x}", Sha256::digest(expected));
        let mut receiver = open_partial_file_receiver_in(
            &partial_dir,
            "transfer-1".to_string(),
            "notes.txt".to_string(),
            expected.len() as u64,
            hash,
        )
        .unwrap();
        receiver
            .file
            .as_mut()
            .unwrap()
            .write_all(b"corrupt bytes")
            .unwrap();
        receiver.received_size = expected.len() as u64;
        let partial_path = receiver.partial_path.clone();
        assert!(finish_partial_file_receive_in(receiver, &downloads_dir).is_err());
        assert!(!partial_path.exists());
        assert!(!downloads_dir.exists());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn diagnostic_events_are_fixed_non_sensitive_categories() {
        assert_eq!(
            DiagnosticEvent::ConnectionStarted.as_str(),
            "connection_started"
        );
        assert_eq!(
            DiagnosticEvent::FileTransferCompleted.as_str(),
            "file_transfer_completed"
        );
        assert_eq!(
            DiagnosticEvent::ClipboardSyncFailed.as_str(),
            "clipboard_sync_failed"
        );
        assert_eq!(
            DiagnosticEvent::AgentConnectionInterrupted.as_str(),
            "agent_connection_interrupted"
        );
        assert_eq!(
            DiagnosticEvent::AgentStartFailed.as_str(),
            "agent_start_failed"
        );
        assert_eq!(
            DiagnosticEvent::AudioCaptureStarted.as_str(),
            "audio_capture_started"
        );
        assert_eq!(
            DiagnosticEvent::AudioOutboundRtpStats.as_str(),
            "audio_outbound_rtp_stats"
        );
        assert_eq!(
            DiagnosticEvent::AudioCaptureFailed.as_str(),
            "audio_capture_failed"
        );
        assert!(
            serde_json::from_str::<DiagnosticEvent>(r#""device_address:192.168.1.2""#).is_err()
        );
    }

    #[test]
    fn diagnostic_log_records_only_fixed_events_and_numeric_metrics() {
        let directory =
            std::env::temp_dir().join(format!("lantern-log-metrics-{}", Uuid::new_v4()));
        let path = directory.join("lantern.log");

        append_diagnostic_log_file_values(
            &path,
            DiagnosticEvent::AudioOutboundRtpStats,
            Some(12),
            Some(4096),
        )
        .unwrap();

        let contents = fs::read_to_string(&path).unwrap();
        let mut fields = contents.trim().split('\t');
        let timestamp = fields.next().unwrap();
        assert!(timestamp.parse::<u128>().is_ok());
        assert_eq!(fields.next(), Some("audio_outbound_rtp_stats"));
        assert_eq!(fields.next(), Some("value=12"));
        assert_eq!(fields.next(), Some("value2=4096"));
        assert_eq!(fields.next(), None);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn diagnostic_log_rotation_preserves_only_the_current_and_previous_files() {
        let directory = std::env::temp_dir().join(format!("lantern-log-test-{}", Uuid::new_v4()));
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join("lantern.log");
        fs::write(&path, "x".repeat(MAX_DIAGNOSTIC_LOG_SIZE as usize)).unwrap();

        append_diagnostic_log_file(&path, DiagnosticEvent::ConnectionStarted).unwrap();

        let contents = read_diagnostic_log_file(&path).unwrap();
        assert!(contents.starts_with(&"x".repeat(MAX_DIAGNOSTIC_LOG_SIZE as usize)));
        assert!(contents.ends_with("\tconnection_started\n"));
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn discovery_names_are_trimmed_and_reject_control_characters() {
        assert_eq!(discovery_name("  devbox  "), Some("devbox".to_string()));
        assert_eq!(discovery_name(" \n "), None);
        assert_eq!(discovery_name("dev\u{0007}box"), None);
        assert_eq!(discovery_name(&"x".repeat(60)).unwrap().len(), 48);
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
