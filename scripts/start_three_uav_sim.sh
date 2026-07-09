#!/usr/bin/env bash
set -euo pipefail

# Start the three-UAV ROS/Gazebo simulation stack:
#   1. PX4 multi-UAV MAVROS SITL
#   2. aofe_star three-UAV task/trajectory/controller sim
#
# The script keeps all child processes attached to this terminal. Press Ctrl-C
# here to stop the ROS launch files started by this script.
#
# QGroundControl and the RadioMaster axis-to-buttons helper are intentionally
# split into scripts/start_qgc_radiomaster.sh because the helper needs sudo.

ROS_DISTRO="${ROS_DISTRO:-noetic}"
CATKIN_WS="${CATKIN_WS:-$HOME/catkin_ws}"
AOFESTAR_LAUNCH="${AOFESTAR_LAUNCH:-sys_coop_lift_test_001_three_uav_sim.launch}"
PX4_DIR="${PX4_DIR:-$HOME/PX4-Autopilot}"
PX4_BUILD_DIR="${PX4_BUILD_DIR:-$PX4_DIR/build/px4_sitl_default}"
SITL_GAZEBO_CLASSIC_DIR="${SITL_GAZEBO_CLASSIC_DIR:-$PX4_DIR/Tools/simulation/gazebo-classic/sitl_gazebo-classic}"
PX4_LAUNCH="${PX4_LAUNCH:-multi_uav_mavros_sitl.launch}"

PIDS=()

log() {
    printf '[three-uav-sim] %s\n' "$*"
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

source_if_exists() {
    local setup_file="$1"
    if [[ -f "$setup_file" ]]; then
        # shellcheck disable=SC1090
        source "$setup_file"
    else
        log "warning: setup file not found: $setup_file"
    fi
}

prepend_path() {
    local var_name="$1"
    local path_value="$2"
    local current_value="${!var_name:-}"

    if [[ ! -d "$path_value" ]]; then
        log "warning: path for $var_name not found: $path_value"
        return
    fi

    if [[ -z "$current_value" ]]; then
        export "$var_name=$path_value"
    else
        export "$var_name=$path_value:$current_value"
    fi
}

setup_px4_gazebo_classic_env() {
    # multi_uav_mavros_sitl.launch references the ROS package
    # mavlink_sitl_gazebo and Gazebo Classic models/plugins from PX4.
    # Set them explicitly so this script works from a clean terminal.
    prepend_path ROS_PACKAGE_PATH "$PX4_DIR"
    prepend_path ROS_PACKAGE_PATH "$SITL_GAZEBO_CLASSIC_DIR"
    prepend_path GAZEBO_MODEL_PATH "$SITL_GAZEBO_CLASSIC_DIR/models"
    prepend_path GAZEBO_PLUGIN_PATH "$PX4_BUILD_DIR/build_gazebo-classic"
    prepend_path LD_LIBRARY_PATH "$PX4_BUILD_DIR/build_gazebo-classic"
}

wait_for_ros_master() {
    local timeout_s="${1:-30}"
    local start_s
    start_s="$(date +%s)"

    while true; do
        if rosnode list >/dev/null 2>&1; then
            return 0
        fi

        if (( "$(date +%s)" - start_s >= timeout_s )); then
            return 1
        fi

        sleep 1
    done
}

source_if_exists "/opt/ros/$ROS_DISTRO/setup.bash"
source_if_exists "$CATKIN_WS/devel/setup.bash"
setup_px4_gazebo_classic_env

log "starting PX4 SITL: roslaunch px4 $PX4_LAUNCH"
roslaunch px4 "$PX4_LAUNCH" &
PIDS+=("$!")

if wait_for_ros_master 45; then
    log "ROS master is ready"
else
    log "warning: ROS master did not become ready within timeout; starting aofe_star launch anyway"
fi

log "starting aofe_star three-UAV sim: roslaunch aofe_star $AOFESTAR_LAUNCH"
roslaunch aofe_star "$AOFESTAR_LAUNCH" &
PIDS+=("$!")

log "all requested processes started. Press Ctrl-C here to stop them."
wait
