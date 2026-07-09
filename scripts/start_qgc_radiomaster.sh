#!/usr/bin/env bash
set -euo pipefail

# Start the operator-side tools for simulation:
#   1. RadioMaster axis-to-buttons helper, via sudo because it uses /dev/uinput
#   2. QGroundControl
#
# Run this in a separate terminal from scripts/start_three_uav_sim.sh.
# Press Ctrl-C here to stop the helper and QGC processes started by this script.

QGC_APPIMAGE="${QGC_APPIMAGE:-}"
START_QGC="${START_QGC:-true}"
START_RADIOMASTER_AXIS_TO_BUTTONS="${START_RADIOMASTER_AXIS_TO_BUTTONS:-true}"
RADIOMASTER_INPUT="${RADIOMASTER_INPUT:-}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RADIOMASTER_AXIS_TOOL="${RADIOMASTER_AXIS_TOOL:-$REPO_ROOT/tools/radiomaster_axis_to_buttons}"

PIDS=()

log() {
    printf '[qgc-radiomaster] %s\n' "$*"
}

cleanup() {
    log "stopping child processes..."
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" >/dev/null 2>&1; then
            kill "$pid" >/dev/null 2>&1 || true
        fi
    done

    for pid in "${PIDS[@]}"; do
        wait "$pid" >/dev/null 2>&1 || true
    done
}

trap cleanup EXIT INT TERM

find_qgc() {
    local candidates=(
        "$HOME/Application/QGrounControl/QGroundControl.AppImage"
        "$HOME/Application/QGroundControl/QGroundControl.AppImage"
        "$HOME/Applications/QGrounControl/QGroundControl.AppImage"
        "$HOME/Applications/QGroundControl/QGroundControl.AppImage"
    )

    if [[ -n "$QGC_APPIMAGE" ]]; then
        candidates=("$QGC_APPIMAGE" "${candidates[@]}")
    fi

    local path
    for path in "${candidates[@]}"; do
        if [[ -f "$path" ]]; then
            printf '%s\n' "$path"
            return 0
        fi
    done

    return 1
}

start_radiomaster_helper() {
    if [[ "$START_RADIOMASTER_AXIS_TO_BUTTONS" != "true" ]]; then
        log "RadioMaster helper disabled by START_RADIOMASTER_AXIS_TO_BUTTONS=$START_RADIOMASTER_AXIS_TO_BUTTONS"
        return
    fi

    if [[ ! -x "$RADIOMASTER_AXIS_TOOL" ]]; then
        log "warning: RadioMaster helper not executable or not found: $RADIOMASTER_AXIS_TOOL"
        return
    fi

    log "requesting sudo for RadioMaster helper (/dev/uinput access)"
    sudo -v

    log "starting RadioMaster axis-to-buttons helper: $RADIOMASTER_AXIS_TOOL"
    if [[ -n "$RADIOMASTER_INPUT" ]]; then
        sudo "$RADIOMASTER_AXIS_TOOL" "$RADIOMASTER_INPUT" >/tmp/radiomaster_axis_to_buttons_three_uav_sim.log 2>&1 &
    else
        sudo "$RADIOMASTER_AXIS_TOOL" >/tmp/radiomaster_axis_to_buttons_three_uav_sim.log 2>&1 &
    fi
    PIDS+=("$!")

    # Give uinput a moment to expose the virtual joystick before QGC scans devices.
    sleep 1
}

start_qgc() {
    if [[ "$START_QGC" != "true" ]]; then
        log "QGroundControl disabled by START_QGC=$START_QGC"
        return
    fi

    if qgc_path="$(find_qgc)"; then
        log "starting QGroundControl: $qgc_path"
        chmod +x "$qgc_path" >/dev/null 2>&1 || true
        "$qgc_path" >/tmp/qgroundcontrol_three_uav_sim.log 2>&1 &
        PIDS+=("$!")
    else
        log "warning: QGroundControl AppImage not found; set QGC_APPIMAGE=/path/to/QGroundControl.AppImage to override"
    fi
}

start_radiomaster_helper
start_qgc

log "operator tools started. Press Ctrl-C here to stop them."
wait
