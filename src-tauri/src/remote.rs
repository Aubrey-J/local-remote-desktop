use std::{
    collections::HashSet,
    net::{IpAddr, SocketAddr},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

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
use tauri::{AppHandle, Emitter};
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::timeout,
};
use uuid::Uuid;

const DEFAULT_PORT: u16 = 4200;
const MAX_FRAME_WIDTH: u32 = 1920;
const FRAME_INTERVAL: Duration = Duration::from_millis(33);
const APPROVAL_TIMEOUT: Duration = Duration::from_secs(60);
const SIGNALING_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Default)]
pub struct RemoteState {
    server: Mutex<Option<JoinHandle<()>>>,
    pending_approval: Mutex<Option<(String, oneshot::Sender<bool>)>>,
    pending_request: Mutex<Option<ConnectionRequest>>,
    webrtc_factory: Mutex<Option<Arc<PeerConnectionFactory>>>,
    active_connection: AtomicBool,
    stopping: AtomicBool,
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
    remote: tauri::State<'_, Arc<RemoteState>>,
    request_id: String,
    approved: bool,
) -> Result<(), String> {
    let mut pending = remote
        .pending_approval
        .lock()
        .map_err(|_| "连接授权状态不可用".to_string())?;
    match pending.take() {
        Some((pending_id, sender)) if pending_id == request_id => {
            remote
                .pending_request
                .lock()
                .map_err(|_| "连接请求状态不可用".to_string())?
                .take();
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
    ws.max_message_size(64 * 1024)
        .max_frame_size(64 * 1024)
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

    let controller_name = match timeout(Duration::from_secs(5), socket.recv()).await {
        Ok(Some(Ok(Message::Text(text)))) => match serde_json::from_str::<ClientMessage>(&text) {
            Ok(ClientMessage::Hello { device_name }) => {
                let name: String = device_name.trim().chars().take(48).collect();
                if name.is_empty() {
                    "局域网控制端".to_string()
                } else {
                    name
                }
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
    let request = ConnectionRequest {
        request_id: request_id.clone(),
        peer_address: peer.to_string(),
        controller_name: controller_name.clone(),
    };
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
    if !approved {
        let _ = state
            .app
            .emit("remote-connection-request-ended", request_id.clone());
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

    let session_error = match run_webrtc_session(&mut socket, &state).await {
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
    ) {
        if self.thread.is_some() {
            return;
        }
        let stopping = Arc::clone(&self.stopping);
        self.thread = Some(thread::spawn(move || {
            while !stopping.load(Ordering::Acquire) {
                let started = std::time::Instant::now();
                match capture_bgra() {
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
                thread::sleep(FRAME_INTERVAL.saturating_sub(started.elapsed()));
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

async fn run_webrtc_session(socket: &mut WebSocket, state: &ServerState) -> Result<(), String> {
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
        let mut pressed_keys = HashSet::new();
        let mut pressed_buttons = HashSet::new();
        let mut scroll_remainder = 0.0;
        for command in input_receiver {
            if let Err(error) = apply_input(
                &mut enigo,
                command,
                width,
                height,
                &mut pressed_keys,
                &mut pressed_buttons,
                &mut scroll_remainder,
            ) {
                let _ = input_error_sender.send(error);
                break;
            }
        }
        for key in pressed_keys {
            let _ = enigo.key(key, Direction::Release);
        }
        for button in pressed_buttons {
            let _ = enigo.button(button, Direction::Release);
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
    peer_connection
        .set_bitrate(None, Some(4_000_000), Some(12_000_000))
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
                            _ => return Err("信令通道收到无效消息".to_string()),
                        }
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
                        capture_worker.start(Arc::clone(&video_track), signal_sender.clone());
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

fn capture_bgra() -> Result<(Vec<u8>, u32, u32), String> {
    let screen = primary_screen()?;
    let screenshot = screen
        .capture()
        .map_err(|error| format!("屏幕采集失败：{error}"))?;
    let width = screenshot.width();
    let height = screenshot.height();
    let pixels = image::RgbaImage::from_raw(width, height, screenshot.into_raw())
        .ok_or_else(|| "屏幕图像格式无效".to_string())?;

    let scale = (MAX_FRAME_WIDTH as f64 / width as f64).min(1.0);
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

fn apply_input(
    enigo: &mut Enigo,
    message: ClientMessage,
    width: u32,
    height: u32,
    pressed_keys: &mut HashSet<Key>,
    pressed_buttons: &mut HashSet<Button>,
    scroll_remainder: &mut f64,
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
                pressed_buttons.insert(button);
                Direction::Press
            } else {
                pressed_buttons.remove(&button);
                Direction::Release
            };
            enigo
                .button(button, direction)
                .map_err(|error| error.to_string())?;
        }
        ClientMessage::Key { code, key, down } => {
            if let Some(key) = translate_key(&code, &key) {
                let direction = if down {
                    pressed_keys.insert(key);
                    Direction::Press
                } else {
                    pressed_keys.remove(&key);
                    Direction::Release
                };
                enigo
                    .key(key, direction)
                    .map_err(|error| error.to_string())?;
            }
        }
        ClientMessage::MouseScroll { delta_y } if delta_y.is_finite() => {
            *scroll_remainder += delta_y;
            let steps = (*scroll_remainder / 100.0).trunc() as i32;
            if steps != 0 {
                *scroll_remainder -= steps as f64 * 100.0;
                enigo
                    .scroll(steps, Axis::Vertical)
                    .map_err(|error| error.to_string())?;
            }
        }
        ClientMessage::Hello { .. } => {}
        ClientMessage::ReleaseAll => {
            for key in pressed_keys.drain() {
                enigo
                    .key(key, Direction::Release)
                    .map_err(|error| error.to_string())?;
            }
            for button in pressed_buttons.drain() {
                enigo
                    .button(button, Direction::Release)
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
        #[cfg(target_os = "windows")]
        "MetaLeft" | "MetaRight" => Some(Key::Control),
        #[cfg(not(target_os = "windows"))]
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
    #[cfg(target_os = "windows")]
    let special = windows_physical_key(code).or(special);
    special.or_else(|| {
        let mut characters = key.chars();
        let character = characters.next()?;
        (characters.next().is_none() && !character.is_control()).then_some(Key::Unicode(character))
    })
}

#[cfg(target_os = "windows")]
fn windows_physical_key(code: &str) -> Option<Key> {
    let key = match code {
        "KeyA" => Key::A,
        "KeyB" => Key::B,
        "KeyC" => Key::C,
        "KeyD" => Key::D,
        "KeyE" => Key::E,
        "KeyF" => Key::F,
        "KeyG" => Key::G,
        "KeyH" => Key::H,
        "KeyI" => Key::I,
        "KeyJ" => Key::J,
        "KeyK" => Key::K,
        "KeyL" => Key::L,
        "KeyM" => Key::M,
        "KeyN" => Key::N,
        "KeyO" => Key::O,
        "KeyP" => Key::P,
        "KeyQ" => Key::Q,
        "KeyR" => Key::R,
        "KeyS" => Key::S,
        "KeyT" => Key::T,
        "KeyU" => Key::U,
        "KeyV" => Key::V,
        "KeyW" => Key::W,
        "KeyX" => Key::X,
        "KeyY" => Key::Y,
        "KeyZ" => Key::Z,
        "Digit0" => Key::Num0,
        "Digit1" => Key::Num1,
        "Digit2" => Key::Num2,
        "Digit3" => Key::Num3,
        "Digit4" => Key::Num4,
        "Digit5" => Key::Num5,
        "Digit6" => Key::Num6,
        "Digit7" => Key::Num7,
        "Digit8" => Key::Num8,
        "Digit9" => Key::Num9,
        "Minus" => Key::OEMMinus,
        "Equal" => Key::OEMPlus,
        "BracketLeft" => Key::OEM4,
        "BracketRight" => Key::OEM6,
        "Backslash" => Key::OEM5,
        "Semicolon" => Key::OEM1,
        "Quote" => Key::OEM7,
        "Backquote" => Key::OEM3,
        "Comma" => Key::OEMComma,
        "Period" => Key::OEMPeriod,
        "Slash" => Key::OEM2,
        _ => return None,
    };
    Some(key)
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[cfg(target_os = "windows")]
    #[test]
    fn windows_shortcuts_use_physical_keys_and_map_command_to_control() {
        assert_eq!(translate_key("MetaLeft", "Meta"), Some(Key::Control));
        assert_eq!(translate_key("KeyC", "c"), Some(Key::C));
        assert_eq!(translate_key("Digit5", "5"), Some(Key::Num5));
    }
}
