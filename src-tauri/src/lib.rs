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
            remote::answer_connection_request
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
