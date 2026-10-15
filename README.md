# Lantern Desktop

Lantern is a LAN-first desktop application for viewing and controlling another computer from a workspace. The controller and controlled computer run the same Tauri 2 application.

## Remote-control preview

1. Build and launch Lantern on both computers with `npm install` and `npm run tauri dev`, or install a desktop bundle.
2. On the computer to be controlled, select **启动被控端**. Lantern listens on TCP port `4200` and displays the LAN address it detects.
3. On the controller, add that private LAN address (for example, `192.168.1.24:4200`) and choose **连接控制**.
4. Approve the incoming request on the controlled computer. Only then does Lantern start sending the primary screen and accept mouse/keyboard events.

The preview sends JPEG screen frames over a direct WebSocket connection at up to 10 frames per second, capped at 1920 pixels wide. It accepts IPv4 LAN addresses and captures the primary screen; multi-monitor selection, IPv6, hardware video encoding, adaptive quality, audio, clipboard, discovery, accounts, file transfer, and Internet/relay connectivity are not implemented. Mouse movement, buttons, wheel scrolling, common shortcuts, and function keys are forwarded. This is an initial functional path, not yet a low-latency production streaming codec.

Connections are limited to loopback, private, and link-local IP addresses; the controlled computer also requires local approval for each connection. The WebSocket stream is not encrypted or authenticated, so use it only on a trusted LAN. Do not expose port 4200 to the Internet. Disconnecting releases held keys and mouse buttons.

### Operating-system permissions and limitations

- **macOS:** grant Lantern Screen Recording and Accessibility permissions in System Settings. Depending on the OS version, restart Lantern after changing permissions.
- **Windows:** standard desktop capture and input are supported by the underlying libraries; the secure desktop/UAC prompt cannot be controlled.
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
