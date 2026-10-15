use std::{
    collections::HashSet,
    net::{IpAddr, SocketAddr},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
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
use image::{ExtendedColorType, RgbImage, imageops::FilterType};
use serde::{Deserialize, Serialize};
use tauri::{AppHandle, Emitter};
use tokio::{
    net::TcpListener,
    sync::oneshot,
    task::JoinHandle,
    time::{interval, timeout},
};
use uuid::Uuid;

const DEFAULT_PORT: u16 = 4200;
const MAX_FRAME_WIDTH: u32 = 1920;
const FRAME_INTERVAL: Duration = Duration::from_millis(100);
const APPROVAL_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Default)]
pub struct RemoteState {
    server: Mutex<Option<JoinHandle<()>>>,
    pending_approval: Mutex<Option<(String, oneshot::Sender<bool>)>>,
    active_connection: AtomicBool,
    stopping: AtomicBool,
}

#[derive(Clone)]
struct ServerState {
    app: AppHandle,
    remote: Arc<RemoteState>,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ConnectionRequest {
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
    Ok(())
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
    ws.max_message_size(16 * 1024)
        .max_frame_size(16 * 1024)
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

    let (width, height) = match tokio::task::spawn_blocking(capture_dimensions).await {
        Ok(Ok(dimensions)) => dimensions,
        Ok(Err(error)) => {
            let _ = socket
                .send(Message::Text(
                    serde_json::json!({"type": "error", "message": error})
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
        Err(error) => {
            let _ = socket
                .send(Message::Text(
                    serde_json::json!({"type": "error", "message": error.to_string()})
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
    };

    let metadata = serde_json::json!({
        "type": "connected",
        "controllerName": controller_name,
        "screenWidth": width,
        "screenHeight": height,
    });
    let (input_sender, input_receiver) = std::sync::mpsc::channel::<ClientMessage>();
    let (ready_sender, ready_receiver) = std::sync::mpsc::sync_channel(1);
    let (input_error_sender, mut input_error_receiver) = tokio::sync::mpsc::channel(1);
    std::thread::spawn(move || {
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
                let _ = input_error_sender.blocking_send(error);
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
            let _ = socket
                .send(Message::Text(
                    serde_json::json!({
                        "type": "error",
                        "message": format!("无法启用键鼠控制：{error}，请检查系统辅助功能权限。")
                    })
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
        Err(error) => {
            let _ = socket
                .send(Message::Text(
                    serde_json::json!({"type": "error", "message": error.to_string()})
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
    }
    if socket
        .send(Message::Text(metadata.to_string().into()))
        .await
        .is_err()
    {
        state
            .remote
            .active_connection
            .store(false, Ordering::Release);
        return;
    }

    let mut frame_clock = interval(FRAME_INTERVAL);
    frame_clock.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            incoming = socket.recv() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        let command = match serde_json::from_str::<ClientMessage>(&text) {
                            Ok(command) => command,
                            Err(_) => {
                                let _ = socket.send(Message::Text(
                                    serde_json::json!({"type": "error", "message": "无效的控制指令"}).to_string().into()
                                )).await;
                                break;
                            }
                        };
                        if matches!(&command, ClientMessage::Hello { .. }) {
                            continue;
                        }
                        if input_sender.send(command).is_err() {
                            let _ = socket.send(Message::Text(
                                serde_json::json!({"type": "error", "message": "键鼠控制服务已停止"}).to_string().into()
                            )).await;
                            break;
                        }
                    }
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                    Some(Ok(Message::Ping(payload))) => {
                        if socket.send(Message::Pong(payload)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(_)) => {}
                }
            }
            _ = frame_clock.tick() => {
                if state.remote.stopping.load(Ordering::Acquire) {
                    let _ = socket.send(Message::Close(None)).await;
                    break;
                }
                match tokio::task::spawn_blocking(capture_jpeg).await {
                    Ok(Ok(frame)) => {
                        if socket.send(Message::Binary(frame.into())).await.is_err() {
                            break;
                        }
                    }
                    Ok(Err(error)) => {
                        let _ = socket.send(Message::Text(
                            serde_json::json!({"type": "error", "message": error}).to_string().into()
                        )).await;
                        break;
                    }
                    Err(error) => {
                        let _ = state.app.emit("agent-status", format!("屏幕采集失败：{error}"));
                        break;
                    }
                }
            }
            Some(error) = input_error_receiver.recv() => {
                let message = format!("键鼠控制失败：{error}，请检查被控端的辅助功能权限。");
                let _ = state.app.emit("agent-status", message.clone());
                let _ = socket.send(Message::Text(
                    serde_json::json!({"type": "error", "message": message}).to_string().into()
                )).await;
                break;
            }
        }
    }
    state
        .remote
        .active_connection
        .store(false, Ordering::Release);
    let _ = state.app.emit("agent-status", "控制端已断开".to_string());
}

fn clear_pending(remote: &RemoteState, request_id: &str) {
    if let Ok(mut pending) = remote.pending_approval.lock() {
        if pending.as_ref().is_some_and(|(id, _)| id == request_id) {
            pending.take();
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

fn capture_jpeg() -> Result<Vec<u8>, String> {
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
    let rgb: RgbImage = image::DynamicImage::ImageRgba8(resized).to_rgb8();
    let mut output = Vec::with_capacity((frame_width * frame_height) as usize / 2);
    image::codecs::jpeg::JpegEncoder::new_with_quality(&mut output, 65)
        .encode(
            rgb.as_raw(),
            frame_width,
            frame_height,
            ExtendedColorType::Rgb8,
        )
        .map_err(|error| format!("屏幕图像编码失败：{error}"))?;
    Ok(output)
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
    fn keyboard_translation_limits_input_to_keys() {
        assert_eq!(translate_key("Enter", "Enter"), Some(Key::Return));
        assert_eq!(translate_key("KeyA", "a"), Some(Key::Unicode('a')));
        assert_eq!(translate_key("F5", "F5"), Some(Key::F5));
        assert_eq!(translate_key("KeyA", "ab"), None);
    }
}
