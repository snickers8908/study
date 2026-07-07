#!/usr/bin/env python3
"""
Concurrent IOS/NX-OS image puller.

Workflow:
  0. Ask which hosts file to use (IPs listed one per line, same dir as script).
  1. Prompt for username/password (reused for device login AND scp source).
  2. (SCP server-enable step dropped — pull uses device as SCP CLIENT only.)
  3. `show version` -> detect platform + model -> map model to target version.
  4. Ping the scp server IP; if default VRF fails, walk VRFs until one works.
  5. Check if target image already in bootflash / already-running version -> skip.
  6. Show summary table (current -> target) and ask to continue.
  7. Concurrently pull images via `copy scp: bootflash:` (branch per platform).
  8. Confirm images landed in bootflash.
  9. Tell user they're good to go.
"""

import os
import re
import sys
import time
import getpass
import logging
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from netmiko import ConnectHandler
from netmiko.exceptions import NetmikoTimeoutException, NetmikoAuthenticationException


# ---------------------------------------------------------------------------
# Logging / debugging
# ---------------------------------------------------------------------------
# One run-wide logger to console + a master logfile, plus a per-device raw
# transcript so you can see exactly what each switch returned.
DEBUG = False                      # toggled by --debug / prompt in main()
LOG_DIR = None                     # set at runtime: ./logs/<timestamp>/
_LOG_LOCK = threading.Lock()

log = logging.getLogger("iospuller")


def setup_logging(debug: bool):
    """Configure console + master file logging. Returns the run log dir."""
    global DEBUG, LOG_DIR
    DEBUG = debug
    script_dir = os.path.dirname(os.path.abspath(__file__))
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    LOG_DIR = os.path.join(script_dir, "logs", stamp)
    os.makedirs(LOG_DIR, exist_ok=True)

    log.setLevel(logging.DEBUG)
    log.handlers.clear()

    fmt = logging.Formatter(
        "%(asctime)s %(levelname)-7s [%(threadName)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    # Console: INFO by default, DEBUG when --debug. Kept on stderr so it does
    # not fight the live progress chart on stdout.
    ch = logging.StreamHandler(sys.stderr)
    ch.setLevel(logging.DEBUG if debug else logging.INFO)
    ch.setFormatter(fmt)
    log.addHandler(ch)

    # Master file: always full DEBUG.
    fh = logging.FileHandler(os.path.join(LOG_DIR, "run.log"))
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    log.addHandler(fh)

    log.info("Logging to %s", LOG_DIR)
    return LOG_DIR


def device_log(host: str, section: str, text: str):
    """Append a raw transcript chunk to the per-device logfile."""
    if not LOG_DIR:
        return
    path = os.path.join(LOG_DIR, f"{host}.log")
    with _LOG_LOCK:
        with open(path, "a") as f:
            f.write(f"\n{'='*70}\n### {section}  ({datetime.now():%H:%M:%S})\n{'='*70}\n")
            f.write(text if text is not None else "<no output>")
            f.write("\n")


def dbg_send(conn, host, label, command, timing=False, secret=False, **kw):
    """
    Wrapper around Netmiko send that logs the command and its raw output to the
    device transcript. Use for every device interaction so failures are visible.
    Set secret=True to mask the command (e.g. passwords) in all logs.
    """
    shown = "********" if secret else (command if command else "<enter>")
    log.debug("%s >>> %s", host, shown)
    try:
        if timing:
            out = conn.send_command_timing(command, **kw)
        else:
            out = conn.send_command(command, **kw)
    except Exception as e:
        device_log(host, f"{label}  CMD={shown!r}  EXCEPTION", repr(e))
        log.debug("%s !!! exception on %r: %s", host, shown, e)
        raise
    device_log(host, f"{label}  CMD={shown!r}", out)
    return out


# ---------------------------------------------------------------------------
# Model -> target version   (Step 3 table)
# ---------------------------------------------------------------------------
MODEL_TO_VERSION = {
    "C8300":           "17.15.5",
    "C9300-24T":       "17.15.5",
    "C9300-24UX":      "17.15.5",
    "C9200-48T":       "17.15.5",
    "C9200L-24P-4G":   "17.15.5",
    "C9200L-48P-4G":   "17.15.5",
    "C93180YC-FX3":    "10.5(4) M",
    "C93180YC-EX":     "10.3(8) M",
}

# ---------------------------------------------------------------------------
# Model -> image file   (Step 5/7 mapping)
# 9200 family -> cat9k_lite ; 9300 family -> cat9k (full) ; Nexus -> nxos
# ---------------------------------------------------------------------------
MODEL_TO_IMAGE = {
    "C8300":           "c8000be-universalk9.17.15.05.SPA.bin",
    "C9300-24T":       "cat9k_iosxe.17.15.05.SPA.bin",
    "C9300-24UX":      "cat9k_iosxe.17.15.05.SPA.bin",
    "C9200-48T":       "cat9k_lite_iosxe.17.15.05.SPA.bin",
    "C9200L-24P-4G":   "cat9k_lite_iosxe.17.15.05.SPA.bin",
    "C9200L-48P-4G":   "cat9k_lite_iosxe.17.15.05.SPA.bin",
    "C93180YC-FX3":    "nxos64-cs.10.5.4.M.bin",
    "C93180YC-EX":     "nxos64-cs.10.3.8.M.bin",
}

# Approximate image sizes (bytes) — used only to scale the progress bar.
# Adjust to your actual file sizes for a precise bar; the live byte count
# is always real regardless of these hints.
IMAGE_SIZE_HINT = {
    "c8000be-universalk9.17.15.05.SPA.bin": 1_100_000_000,
    "cat9k_iosxe.17.15.05.SPA.bin":       1_300_000_000,
    "cat9k_lite_iosxe.17.15.05.SPA.bin":    700_000_000,
    "nxos64-cs.10.5.4.M.bin":             2_200_000_000,
    "nxos64-cs.10.3.8.M.bin":             2_100_000_000,
}

# Regex that proves a device is already running the target version,
# matched against `show version` text. 17.15.5 may render as 17.15.5 or 17.15.05.
VERSION_RUNNING_TOKEN = {
    "17.15.5":    r"17\.15\.0?5[a-z]?\b",
    "10.3(8) M":  r"10\.3\(8\)",
    "10.5(4) M":  r"10\.5\(4\)",
}


# ---------------------------------------------------------------------------
# Detection helpers
# ---------------------------------------------------------------------------
def detect_platform(show_ver: str) -> str:
    """Return 'nxos' or 'ios'."""
    if re.search(r"NX-?OS", show_ver, re.IGNORECASE) or "Nexus" in show_ver:
        return "nxos"
    return "ios"


def detect_model(show_ver: str, platform: str):
    """Return the model string that matches a key in MODEL_TO_VERSION, or None."""
    # C8300 family: any SKU (C8300-1N1S-4T2X, C8300-2N2S-6T, ...) -> "C8300"
    if re.search(r"C8300", show_ver):
        return "C8300"

    # Direct scan for any known model token first (most reliable).
    for model in MODEL_TO_VERSION:
        if model in show_ver:
            return model

    # Fallback regexes if the exact token wasn't a clean substring.
    if platform == "nxos":
        m = re.search(r"cisco Nexus\S*\s+(C9\d{3}[A-Z0-9-]+)", show_ver)
        if m and m.group(1) in MODEL_TO_VERSION:
            return m.group(1)
    else:
        m = re.search(r"cisco\s+(C9\d{3}[A-Z0-9-]+)", show_ver)
        if m and m.group(1) in MODEL_TO_VERSION:
            return m.group(1)
    return None


def device_fs(conn, platform: str) -> str:
    """
    Return the writable filesystem name including the trailing colon.
    NX-OS always uses bootflash:. IOS-XE prefers bootflash:, falling back
    to flash: if bootflash: isn't present on the box.
    """
    if platform == "nxos":
        return "bootflash:"
    out = conn.send_command("dir bootflash:", read_timeout=30)
    if re.search(r"Invalid input|No such|not found|Error", out, re.IGNORECASE):
        return "flash:"
    if out.strip():
        return "bootflash:"
    # empty/odd response — try flash: to confirm
    out2 = conn.send_command("dir flash:", read_timeout=30)
    if out2.strip() and not re.search(r"Invalid input|No such|Error", out2, re.IGNORECASE):
        return "flash:"
    return "bootflash:"


def bootflash_listing(conn, platform: str, fs: str = None) -> str:
    """List the device filesystem, honoring a detected fs (bootflash:/flash:)."""
    if fs is None:
        fs = device_fs(conn, platform)
    return conn.send_command(f"dir {fs}", read_timeout=30)


def image_present(listing: str, image_file: str) -> bool:
    return image_file in listing


COPY_ERROR_PATTERNS = [
    r"%Error",
    r"Error opening",
    r"Connection refused",
    r"Connection timed out",
    r"Permission denied",
    r"Authentication failed",
    r"No such file or directory",
    r"Not enough space",
    r"No space left",
    r"Invalid input detected",
    r"Host key verification failed",
    r"Aborted",
    r"failed to copy",
    r"Copy failed",
    r"Signature.{0,20}failed",
]


def detect_copy_error(transcript: str):
    """
    Return the first device-reported copy error found in the transcript, or None.
    These are errors the device prints but Netmiko does not raise on.
    """
    for pat in COPY_ERROR_PATTERNS:
        m = re.search(pat, transcript, re.IGNORECASE)
        if m:
            start = max(0, m.start() - 20)
            end = min(len(transcript), m.end() + 60)
            snippet = transcript[start:end].replace("\n", " ").strip()
            return snippet
    return None


def already_running(show_ver: str, target_version: str) -> bool:
    pattern = VERSION_RUNNING_TOKEN[target_version]
    return re.search(pattern, show_ver) is not None


# ---------------------------------------------------------------------------
# VRF / ping logic (Step 4)
# ---------------------------------------------------------------------------
def get_vrfs(conn, platform: str):
    """Return a list of VRF names to try (excluding default, which is tried first)."""
    vrfs = []
    if platform == "nxos":
        out = conn.send_command("show vrf all")
        for line in out.splitlines():
            m = re.match(r"^(\S+)\s+\d+\s+\S+", line.strip())
            if m and m.group(1).lower() not in ("vrf-name", "name"):
                vrfs.append(m.group(1))
    else:
        out = conn.send_command("show vrf")
        for line in out.splitlines():
            m = re.match(r"^\s{2,}(\S+)\s", line)
            if m:
                name = m.group(1)
                if name.lower() not in ("name", "interfaces"):
                    vrfs.append(name)
    # de-dupe, drop obvious non-vrf tokens
    seen, clean = set(), []
    for v in vrfs:
        if v not in seen and v not in ("default", "management"):
            seen.add(v)
            clean.append(v)
    return clean


def ping_ok(output: str) -> bool:
    # IOS-XE: "Success rate is X percent (n/m)"; NX-OS: "X packets received"
    if "Success rate is 0" in output:
        return False
    if re.search(r"Success rate is\s+(\d+)\s+percent", output):
        m = re.search(r"Success rate is\s+(\d+)\s+percent", output)
        return int(m.group(1)) > 0
    m = re.search(r"(\d+)\s+packets received", output)
    if m:
        return int(m.group(1)) > 0
    m = re.search(r"(\d+)\.?\d*%\s+packet loss", output)
    if m:
        return float(m.group(1)) < 100
    return False


def find_working_vrf(conn, platform: str, server_ip: str):
    """
    Try default VRF first, then each configured VRF.
    Returns (reachable: bool, vrf_or_None).  vrf None == default/global table.
    """
    default_cmd = f"ping {server_ip}"
    if ping_ok(conn.send_command(default_cmd, read_timeout=30)):
        return True, None

    for vrf in get_vrfs(conn, platform):
        if platform == "nxos":
            cmd = f"ping {server_ip} vrf {vrf}"
        else:
            cmd = f"ping vrf {vrf} {server_ip}"
        if ping_ok(conn.send_command(cmd, read_timeout=30)):
            return True, vrf
    return False, None


# ---------------------------------------------------------------------------
# Progress tracking (live chart during Step 7)
# ---------------------------------------------------------------------------
PROGRESS = {}                 # host -> {cur, total, state}
PROGRESS_LOCK = threading.Lock()
STOP_RENDER = threading.Event()


def set_progress(host, **kw):
    with PROGRESS_LOCK:
        PROGRESS.setdefault(host, {"cur": 0, "total": 0, "state": "queued"})
        PROGRESS[host].update(kw)


def parse_size(listing: str, image_file: str):
    """Return byte size of image_file in a `dir` listing, or 0 if absent."""
    for line in listing.splitlines():
        if image_file in line:
            m = re.search(r"(\d{4,})\s", line)  # size column (>=1000 bytes)
            nums = re.findall(r"\b(\d{5,})\b", line)
            if nums:
                return max(int(n) for n in nums)
    return 0


def human(n):
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.0f}{unit}"
        n /= 1024
    return f"{n:.1f}TB"


def monitor_progress(host, platform, username, password, image_file, total_hint):
    """
    Second read-only connection that polls `dir bootflash:` for the growing
    file size until the pull thread marks this host done/failed.
    """
    dev = {
        "device_type": "cisco_nxos" if platform == "nxos" else "cisco_ios",
        "host": host, "username": username, "password": password,
        "fast_cli": False,
    }
    try:
        mon = ConnectHandler(**dev)
    except Exception:
        return
    fs = device_fs(mon, platform)
    try:
        while True:
            with PROGRESS_LOCK:
                st = PROGRESS.get(host, {}).get("state", "copying")
            if st in ("done", "failed"):
                break
            try:
                listing = mon.send_command(f"dir {fs}", read_timeout=30)
                cur = parse_size(listing, image_file)
                set_progress(host, cur=cur, total=total_hint, state="copying")
            except Exception:
                pass
            if STOP_RENDER.wait(5):
                break
    finally:
        try:
            mon.disconnect()
        except Exception:
            pass


def render_progress():
    """Redraws the live bar chart until STOP_RENDER is set."""
    bar_w = 30
    first = True
    while not STOP_RENDER.is_set():
        with PROGRESS_LOCK:
            snapshot = {h: dict(v) for h, v in PROGRESS.items()}
        lines = []
        for host in sorted(snapshot):
            p = snapshot[host]
            cur, total, state = p["cur"], p["total"], p["state"]
            if state == "done":
                bar = "#" * bar_w
                pct = "100%"
            elif state == "failed":
                bar = "!" * bar_w
                pct = "ERR "
            elif total > 0:
                frac = min(cur / total, 1.0)
                filled = int(frac * bar_w)
                bar = "#" * filled + "-" * (bar_w - filled)
                pct = f"{frac*100:4.0f}%"
            else:
                bar = "-" * bar_w
                pct = "  ? "
            size = f"{human(cur)}/{human(total)}" if total else human(cur)
            lines.append(f"  {host:<16} [{bar}] {pct}  {size:<16} {state}")
        # move cursor up to redraw in place
        if not first:
            sys.stdout.write(f"\033[{len(lines)}A")
        first = False
        sys.stdout.write("\n".join(lines) + "\n")
        sys.stdout.flush()
        STOP_RENDER.wait(1)


# ---------------------------------------------------------------------------
# The concurrent pull (Step 7)
# ---------------------------------------------------------------------------
def pull_image(host, username, password, platform, vrf, image_file, server_ip,
               total_hint=0):
    """Runs in its own thread. Returns (host, ok, message)."""
    dev = {
        "device_type": "cisco_nxos" if platform == "nxos" else "cisco_ios",
        "host": host,
        "username": username,
        "password": password,
        "fast_cli": False,
    }
    try:
        conn = ConnectHandler(**dev)
        log.info("%s: connected (%s)", host, dev["device_type"])
    except Exception as e:
        set_progress(host, state="failed")
        log.error("%s: reconnect failed: %s", host, e)
        device_log(host, "RECONNECT FAILED", repr(e))
        return (host, False, f"reconnect failed: {e}")

    set_progress(host, cur=0, total=total_hint, state="copying")
    mon_t = threading.Thread(
        target=monitor_progress,
        args=(host, platform, username, password, image_file, total_hint),
        daemon=True,
        name=f"mon-{host}",
    )
    mon_t.start()

    try:
        # Determine target filesystem (bootflash: with flash: fallback on IOS-XE)
        fs = device_fs(conn, platform)
        log.info("%s: target filesystem = %s, image = %s, vrf = %s",
                 host, fs, image_file, vrf or "default")

        if platform == "nxos":
            out = dbg_send(conn, host, "copy step: init", "copy scp: bootflash:", timing=True)
            out += dbg_send(conn, host, "copy step: source file", image_file, timing=True)
            out += dbg_send(conn, host, "copy step: vrf", vrf if vrf else "", timing=True)
            out += dbg_send(conn, host, "copy step: scp hostname", server_ip, timing=True)
            out += dbg_send(conn, host, "copy step: username", username, timing=True)
            if re.search(r"fingerprint|continue connecting|yes/no", out, re.IGNORECASE):
                log.debug("%s: accepting host fingerprint", host)
                out += dbg_send(conn, host, "copy step: fingerprint", "yes", timing=True)
            out += dbg_send(conn, host, "copy step: password", password,
                            timing=True, secret=True, read_timeout=600)
            out += dbg_send(conn, host, "copy step: settle", "",
                            timing=True, read_timeout=600)
        else:
            out = dbg_send(conn, host, "copy step: init", f"copy scp: {fs}", timing=True)
            out += dbg_send(conn, host, "copy step: remote host", server_ip, timing=True)
            out += dbg_send(conn, host, "copy step: source username", username, timing=True)
            out += dbg_send(conn, host, "copy step: source filename", image_file, timing=True)
            out += dbg_send(conn, host, "copy step: dest filename", image_file, timing=True)
            if re.search(r"fingerprint|continue connecting|yes/no", out, re.IGNORECASE):
                log.debug("%s: accepting host fingerprint", host)
                out += dbg_send(conn, host, "copy step: fingerprint", "yes", timing=True)
            out += dbg_send(conn, host, "copy step: password", password,
                            timing=True, secret=True, read_timeout=600)
            out += dbg_send(conn, host, "copy step: settle", "",
                            timing=True, read_timeout=600)

        device_log(host, "FULL COPY TRANSCRIPT", out)

        # Detect device-reported copy errors that Netmiko won't raise on.
        err = detect_copy_error(out)
        if err:
            set_progress(host, state="failed")
            log.error("%s: copy error detected: %s", host, err)
            conn.disconnect()
            return (host, False, f"copy error: {err}")

        # Verify (Step 8)
        listing = dbg_send(conn, host, "verify: dir", f"dir {fs}", read_timeout=30)
        conn.disconnect()
        if image_present(listing, image_file):
            set_progress(host, cur=parse_size(listing, image_file), state="done")
            log.info("%s: SUCCESS — %s present in %s", host, image_file, fs)
            return (host, True, f"image present in {fs}")
        set_progress(host, state="failed")
        log.error("%s: %s NOT found in %s after copy", host, image_file, fs)
        return (host, False, f"copy finished but {image_file} not found in {fs}")
    except Exception as e:
        set_progress(host, state="failed")
        log.exception("%s: exception during copy: %s", host, e)
        device_log(host, "COPY EXCEPTION", repr(e))
        try:
            conn.disconnect()
        except Exception:
            pass
        return (host, False, f"error during copy: {e}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))

    debug = ("--debug" in sys.argv) or ("-d" in sys.argv)
    if not debug:
        ans = input("Enable verbose debug logging? (y/N): ").strip().lower()
        debug = ans in ("y", "yes")
    setup_logging(debug)
    log.info("=== IOS puller run started (debug=%s) ===", debug)

    # Step 0 -----------------------------------------------------------------
    hosts_file = input("Enter the hosts filename (in this script's directory): ").strip()
    hosts_path = os.path.join(script_dir, hosts_file)
    if not os.path.isfile(hosts_path):
        log.error("Hosts file not found: %s", hosts_path)
        print(f"File not found: {hosts_path}")
        sys.exit(1)
    with open(hosts_path) as f:
        hosts = [ln.strip() for ln in f if ln.strip()]
    if not hosts:
        log.error("No IPs in hosts file")
        print("No IP addresses found in hosts file.")
        sys.exit(1)
    log.info("Loaded %d host(s): %s", len(hosts), ", ".join(hosts))

    # Step 1 -----------------------------------------------------------------
    username = input("Username: ").strip()
    password = getpass.getpass("Password: ")

    server_ip = input("SCP server IP: ").strip()

    notes = []          # end-of-run notations
    plan = []           # devices that need a pull
    skipped = []        # already-current or already-present

    # Steps 2-5 sequential per device ---------------------------------------
    for host in hosts:
        print(f"\n=== {host} ===")
        log.info("%s: starting assessment", host)
        dev = {
            "device_type": "autodetect",
            "host": host,
            "username": username,
            "password": password,
            "fast_cli": False,
        }
        # Connect (try ios then nxos)
        conn = None
        for dtype in ("cisco_ios", "cisco_nxos"):
            try:
                dev["device_type"] = dtype
                log.debug("%s: connecting as %s", host, dtype)
                conn = ConnectHandler(**dev)
                log.info("%s: connected as %s", host, dtype)
                break
            except NetmikoAuthenticationException as e:
                log.error("%s: auth failed: %s", host, e)
                notes.append(f"{host}: authentication failed ({e}) — skipped")
                conn = None
                break
            except (NetmikoTimeoutException, Exception) as e:
                log.debug("%s: connect as %s failed: %s", host, dtype, e)
                conn = None
                continue
        if conn is None:
            print(f"{host}: unreachable / login failed — skipped")
            log.error("%s: unreachable or login failed", host)
            if not any(host in n for n in notes):
                notes.append(f"{host}: unreachable or login failed — skipped")
            continue

        show_ver = dbg_send(conn, host, "show version", "show version")
        platform = detect_platform(show_ver)
        log.info("%s: detected platform = %s", host, platform)

        # Step 3: model -> target version
        model = detect_model(show_ver, platform)
        if model is None:
            log.error("%s: model not recognized in show version", host)
            notes.append(f"{host}: model not in table — skipped")
            print(f"{host}: model not recognized — skipped")
            conn.disconnect()
            continue
        target_version = MODEL_TO_VERSION[model]
        image_file = MODEL_TO_IMAGE[model]
        log.info("%s: model=%s -> version=%s image=%s",
                 host, model, target_version, image_file)

        # Step 4: ping / vrf
        reachable, vrf = find_working_vrf(conn, platform, server_ip)
        log.info("%s: reachable=%s vrf=%s", host, reachable, vrf or "default")
        if not reachable:
            log.error("%s: SCP server %s unreachable in any VRF", host, server_ip)
            notes.append(f"{host} ({model}): cannot reach SCP server {server_ip} in any VRF — skipped")
            print(f"{host}: SCP server unreachable in any VRF — skipped")
            conn.disconnect()
            continue

        # Step 5: already present / already running?
        listing = bootflash_listing(conn, platform)
        present = image_present(listing, image_file)
        running = already_running(show_ver, target_version)
        log.debug("%s: image_present=%s already_running=%s", host, present, running)
        if present or running:
            reason = "image already in bootflash" if present else "already running target version"
            skipped.append((host, model, target_version, reason))
            log.info("%s: %s — skipping pull", host, reason)
            print(f"{host}: {reason} — skipping pull")
            conn.disconnect()
            continue

        # current running version (best-effort for the summary table)
        cur = "unknown"
        m = re.search(r"(?:NXOS:\s*version|Cisco IOS XE Software, Version|Version)\s+([0-9][0-9A-Za-z().]+)", show_ver)
        if m:
            cur = m.group(1)

        plan.append({
            "host": host, "model": model, "platform": platform,
            "current": cur, "target": target_version,
            "image": image_file, "vrf": vrf,
        })
        conn.disconnect()

    # Step 6: summary + confirm ---------------------------------------------
    print("\n================ UPGRADE PLAN ================")
    if skipped:
        print("\nSkipped (already good):")
        for h, mdl, tv, why in skipped:
            print(f"  {h:<16} {mdl:<16} {tv:<10} — {why}")
    if not plan:
        print("\nNothing to pull.")
        if notes:
            print("\nNotes:")
            for n in notes:
                print(f"  - {n}")
        print("\nDone.")
        return

    print(f"\n{'HOST':<16}{'MODEL':<16}{'CURRENT':<14}{'-> TARGET':<12}{'IMAGE':<40}{'VRF'}")
    for p in plan:
        print(f"{p['host']:<16}{p['model']:<16}{p['current']:<14}{p['target']:<12}{p['image']:<40}{p['vrf'] or 'default'}")

    ans = input("\nWould you like to continue with the pull? (yes/no): ").strip().lower()
    if ans not in ("y", "yes"):
        print("Aborted by user.")
        return

    # Step 7: concurrent pull ------------------------------------------------
    print("\nPulling images concurrently...\n")

    # seed progress state so the chart shows every host from the start
    for p in plan:
        set_progress(p["host"], cur=0,
                     total=IMAGE_SIZE_HINT.get(p["image"], 0), state="queued")

    STOP_RENDER.clear()
    renderer = threading.Thread(target=render_progress, daemon=True)
    renderer.start()

    results = []
    with ThreadPoolExecutor(max_workers=min(len(plan), 20)) as ex:
        futs = {
            ex.submit(pull_image, p["host"], username, password,
                      p["platform"], p["vrf"], p["image"], server_ip,
                      IMAGE_SIZE_HINT.get(p["image"], 0)): p
            for p in plan
        }
        for fut in as_completed(futs):
            host, ok, msg = fut.result()
            results.append((host, ok, msg))

    # stop the live chart and let it draw one final frame
    STOP_RENDER.set()
    renderer.join(timeout=3)
    print()  # spacing after the chart

    # Step 8 already verified inside pull_image; summarize.
    print("\n================ RESULTS ================")
    for host, ok, msg in results:
        print(f"  {host:<16} {'SUCCESS' if ok else 'FAILED '} — {msg}")
        if not ok:
            notes.append(f"{host}: pull failed — {msg}")

    if notes:
        print("\nNotes / notations:")
        for n in notes:
            print(f"  - {n}")

    # Step 9 -----------------------------------------------------------------
    if all(ok for _, ok, _ in results) and not any("failed" in n.lower() for n in notes):
        print("\nAll images pulled and confirmed in bootflash. You're good to go.")
    else:
        print("\nCompleted with exceptions — see notes above.")


if __name__ == "__main__":
    main()
