# Lantern Desktop

Lantern is a LAN-first desktop application for viewing and controlling another computer from a workspace. The controller and controlled computer run the same Tauri 2 application.

## Remote-control preview

1. Build and launch Lantern on both computers with `npm install` and `npm run tauri dev`, or install a desktop bundle.
2. On the computer to be controlled, select **启动被控端**. Lantern listens on TCP port `4200` and displays the LAN address it detects.
3. On the controller, add that private LAN address (for example, `192.168.1.24:4200`) and choose **连接控制**.
4. Approve the incoming request on the controlled computer. Only then does Lantern start sending the primary screen and accept mouse/keyboard events. Focus the remote screen to type; on a Mac controller, Command shortcuts are mapped to Control shortcuts on a Windows controlled computer. Windows receives physical key transitions for standard US-layout alphanumeric, punctuation, modifier, navigation, function, and numpad keys, so chords such as Shift+; produce the shifted key on the controlled computer's active keyboard layout. Use the expand button in the remote view to enter fullscreen, and press Escape or the button again to leave fullscreen.

Added devices can be removed from the three-dot menu on their card; removal requires a second confirmation. The workspace supports grid, tile, and list layouts. In application settings, a controller IP can be reviewed and removed from the trust list. Trust is optional and only added after the controlled computer approves a request with **信任此控制端** checked; later requests from that IP are automatically approved, so only trust devices on a network you control.

Clipboard sharing is opt-in in settings and is manual: the controller must click the clipboard action. The receiving user approves the first clipboard update in each connection; after approval, later updates are written automatically until disconnect. Each new connection asks again. File transfers also require approval on the receiving computer, are limited to 100 MiB, and save to the Downloads/Lantern Received folder without overwriting existing files.

Screen video is sent over a direct WebRTC media connection. In application settings, choose quality-first (up to 2560 pixels wide, 8–20 Mbps), balanced (up to 1920 pixels wide, 4–12 Mbps), or smoothness-first (up to 1600 pixels wide, 16 ms capture interval and 3–12 Mbps). These are target limits; actual frame rate, resolution, and bitrate depend on the controlled display, hardware, and network. The selected mode applies when starting a new connection. Keyboard and mouse input use a WebRTC data channel; the WebSocket on TCP port `4200` is retained for approval and connection signaling. The app prefers H.264 when the platform encoder is available and otherwise falls back to WebRTC's software codec. The live view reports the active codec/encoder and whether the encoder reports hardware acceleration. The initial hardware-encoding path uses VideoToolbox on macOS; Windows and Linux currently use software fallback. Multi-monitor selection, audio, device discovery, accounts, and Internet/relay connectivity are not implemented yet.

Connections are limited to loopback, private, and link-local IP addresses; the controlled computer requires local approval unless that source IP was explicitly trusted. WebRTC encrypts media and control traffic between peers. The initial signaling handshake is LAN-only and has no account authentication, so use it only on a trusted network and do not expose port `4200` to the Internet. Allow TCP port `4200` and UDP ports `4201–4210` through the local firewall on the controlled computer. Disconnecting releases held keys and mouse buttons.

### Operating-system permissions and limitations

- **macOS 13 or later:** required by the bundled WebRTC engine. Grant Lantern Screen Recording and Accessibility permissions in System Settings. Depending on the OS version, restart Lantern after changing permissions.
- **Windows:** standard desktop capture and input are supported by the underlying libraries; the secure desktop/UAC prompt cannot be controlled. Some games and elevated applications may ignore injected mouse events because they use exclusive fullscreen, raw-input paths, integrity-level isolation, or anti-cheat input filtering. Try windowed/borderless mode and run both applications at the same privilege level. Lantern does not bypass anti-cheat or secure-desktop protections.
- **Linux:** screen capture and input depend on the display server and desktop-session permissions. Wayland support varies by compositor and may require an X11 session.

## Development

Install JavaScript dependencies and start the desktop application:

```sh
npm install
npm run tauri dev
```

Build the frontend:

```sh
npm run build
```

Tauri's platform prerequisites are required to build desktop bundles. See the [Tauri prerequisites](https://v2.tauri.app/start/prerequisites/) for Windows, macOS, and Linux.

## Cross-platform builds

The GitHub Actions workflow builds native installers on Linux, Windows, and macOS runners whenever a branch is pushed or a pull request is opened. It also supports manual runs from the repository's **Actions → Build desktop apps → Run workflow** page. Download the `lantern-linux`, `lantern-windows`, or `lantern-macos` artifact from a completed workflow run. Artifacts are retained for 14 days.

The workflow produces Debian and AppImage bundles on Linux, an NSIS installer on Windows, and a DMG on macOS. These are unsigned preview builds; publishing signed installers requires configuring the platform signing credentials in GitHub Actions secrets.

## Planned

Multi-monitor selection, audio, device discovery, accounts, and Internet/relay connectivity are planned for later iterations.
