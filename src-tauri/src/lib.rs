mod remote;

use std::{
    fs,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tauri::{
    Manager, WindowEvent,
    menu::{Menu, MenuItem},
    tray::TrayIconBuilder,
};

fn close_to_tray_path(app: &tauri::AppHandle) -> Result<PathBuf, String> {
    let mut path = app
        .path()
        .app_config_dir()
        .map_err(|error| format!("无法定位应用配置目录：{error}"))?;
    path.push("close-to-tray.json");
    Ok(path)
}

fn read_close_to_tray(app: &tauri::AppHandle) -> Result<bool, String> {
    let path = close_to_tray_path(app)?;
    match fs::read_to_string(path) {
        Ok(value) => {
            serde_json::from_str(&value).map_err(|error| format!("无法读取关闭窗口设置：{error}"))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(error) => Err(format!("无法读取关闭窗口设置：{error}")),
    }
}

#[tauri::command]
fn get_close_to_tray(close_to_tray: tauri::State<'_, Arc<AtomicBool>>) -> bool {
    close_to_tray.load(Ordering::Relaxed)
}

#[tauri::command]
fn set_close_to_tray(
    app: tauri::AppHandle,
    close_to_tray: tauri::State<'_, Arc<AtomicBool>>,
    enabled: bool,
) -> Result<(), String> {
    let path = close_to_tray_path(&app)?;
    let parent = path
        .parent()
        .ok_or_else(|| "无法定位应用配置目录。".to_string())?;
    fs::create_dir_all(parent).map_err(|error| format!("无法创建应用配置目录：{error}"))?;
    let contents = serde_json::to_string(&enabled)
        .map_err(|error| format!("无法保存关闭窗口设置：{error}"))?;
    fs::write(path, contents).map_err(|error| format!("无法保存关闭窗口设置：{error}"))?;
    close_to_tray.store(enabled, Ordering::Relaxed);
    Ok(())
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let remote_state = std::sync::Arc::new(remote::RemoteState::default());
    let close_to_tray = Arc::new(AtomicBool::new(true));
    let close_to_tray_for_close = Arc::clone(&close_to_tray);
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .manage(remote_state)
        .manage(Arc::clone(&close_to_tray))
        .setup(move |app| {
            match read_close_to_tray(app.handle()) {
                Ok(enabled) => close_to_tray.store(enabled, Ordering::Relaxed),
                Err(error) => eprintln!("{error}"),
            }

            let show = MenuItem::with_id(app, "show", "显示 Lantern", true, None::<&str>)?;
            let quit = MenuItem::with_id(app, "quit", "退出 Lantern", true, None::<&str>)?;
            let menu = Menu::with_items(app, &[&show, &quit])?;
            let icon = tauri::image::Image::from_bytes(include_bytes!("../icons/icon.png"))?;
            TrayIconBuilder::new()
                .icon(icon)
                .menu(&menu)
                .tooltip("Lantern")
                .on_menu_event(|app, event| match event.id().as_ref() {
                    "show" => {
                        if let Some(window) = app.get_webview_window("main") {
                            if let Err(error) = window.show() {
                                eprintln!("无法显示 Lantern 窗口：{error}");
                            }
                            if let Err(error) = window.set_focus() {
                                eprintln!("无法聚焦 Lantern 窗口：{error}");
                            }
                        }
                    }
                    "quit" => app.exit(0),
                    _ => {}
                })
                .build(app)?;
            Ok(())
        })
        .on_window_event(move |window, event| {
            if window.label() == "main"
                && let WindowEvent::CloseRequested { api, .. } = event
                && close_to_tray_for_close.load(Ordering::Relaxed)
            {
                api.prevent_close();
                if let Err(error) = window.hide() {
                    eprintln!("无法隐藏 Lantern 窗口：{error}");
                }
            }
        })
        .invoke_handler(tauri::generate_handler![
            remote::start_agent,
            remote::stop_agent,
            remote::get_pending_connection_request,
            remote::answer_connection_request,
            remote::get_local_device_info,
            remote::get_trusted_devices,
            remote::remove_trusted_device,
            remote::answer_file_transfer,
            get_close_to_tray,
            set_close_to_tray
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
