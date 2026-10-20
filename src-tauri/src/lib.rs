mod remote;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let remote_state = std::sync::Arc::new(remote::RemoteState::default());
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .manage(remote_state)
        .invoke_handler(tauri::generate_handler![
            remote::start_agent,
            remote::stop_agent,
            remote::get_pending_connection_request,
            remote::answer_connection_request,
            remote::get_local_device_info,
            remote::get_trusted_devices,
            remote::remove_trusted_device,
            remote::answer_file_transfer
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
