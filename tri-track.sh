#!/usr/bin/env bash
#
# Start, stop and check the Tri-Tracker web app.
#
#   ./tri-track.sh start     run in the background and return the terminal
#   ./tri-track.sh stop      stop it
#   ./tri-track.sh restart   stop, then start
#   ./tri-track.sh status    is it running, and on what
#   ./tri-track.sh logs      follow the log (Ctrl-C just stops following)
#
set -uo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="$APP_DIR/.tri-track.pid"
LOG_FILE="$APP_DIR/tri-track.log"
PORT="${PORT:-5050}"
URL="http://localhost:$PORT"

cd "$APP_DIR" || exit 1

python_bin() {
    for candidate in "${PYTHON:-}" python3 python; do
        [ -n "$candidate" ] && command -v "$candidate" >/dev/null 2>&1 && { echo "$candidate"; return 0; }
    done
    echo "Could not find python3 on PATH. Install Python 3.10+ or set PYTHON=/path/to/python" >&2
    return 1
}

running_pid() {
    # A PID file alone is not proof: the process may have died, or the id reused.
    [ -f "$PID_FILE" ] || return 1
    local pid; pid="$(cat "$PID_FILE" 2>/dev/null)"
    [ -n "$pid" ] || return 1
    ps -p "$pid" -o command= 2>/dev/null | grep -q "app.py" || return 1
    echo "$pid"
}

port_pid() {
    lsof -nP -iTCP:"$PORT" -sTCP:LISTEN -t 2>/dev/null | head -1
}

check_env() {
    if [ ! -f "$APP_DIR/.env" ]; then
        echo "No .env found."
        if [ -f "$APP_DIR/.env.example" ]; then
            cp "$APP_DIR/.env.example" "$APP_DIR/.env"
            echo "Created .env from .env.example — add your API tokens, then run start again."
            echo "  (or start the app and use the gear icon in the top right)"
        fi
        return 1
    fi
    return 0
}

start() {
    local pid
    if pid="$(running_pid)"; then
        echo "Already running (pid $pid) at $URL"
        return 0
    fi
    if [ -n "$(port_pid)" ]; then
        echo "Port $PORT is already in use by pid $(port_pid)."
        echo "Stop that process, or start this one on another port:  PORT=5060 ./tri-track.sh start"
        return 1
    fi

    local py; py="$(python_bin)" || return 1
    if ! "$py" -c "import flask, databricks.sdk, dotenv, requests" >/dev/null 2>&1; then
        echo "Installing Python dependencies…"
        "$py" -m pip install -r "$APP_DIR/requirements.txt" || {
            echo "Dependency install failed. Try: $py -m pip install -r requirements.txt" >&2
            return 1
        }
    fi

    check_env || true   # the app still starts; settings can be filled in from the UI

    echo "Starting Tri-Tracker…"
    PORT="$PORT" nohup "$py" "$APP_DIR/app.py" >>"$LOG_FILE" 2>&1 &
    local new_pid=$!
    echo "$new_pid" > "$PID_FILE"

    # Wait for it to answer rather than claiming success straight away.
    for _ in $(seq 1 30); do
        if curl -fsS -o /dev/null "$URL" 2>/dev/null; then
            echo "Running at $URL  (pid $new_pid)"
            echo "Logs: $LOG_FILE"
            return 0
        fi
        ps -p "$new_pid" >/dev/null 2>&1 || break
        sleep 1
    done

    echo "It did not come up. Last lines of $LOG_FILE:" >&2
    tail -n 15 "$LOG_FILE" >&2
    rm -f "$PID_FILE"
    return 1
}

stop() {
    local pid; pid="$(running_pid)" || pid=""
    if [ -z "$pid" ]; then
        # Fall back to whatever holds the port, e.g. started by hand with python3 app.py
        pid="$(port_pid)"
        [ -n "$pid" ] && echo "No pid file; stopping the process on port $PORT instead."
    fi
    if [ -z "$pid" ]; then
        echo "Not running."
        rm -f "$PID_FILE"
        return 0
    fi

    echo "Stopping (pid $pid)…"
    kill "$pid" 2>/dev/null
    for _ in $(seq 1 15); do
        ps -p "$pid" >/dev/null 2>&1 || break
        sleep 1
    done
    if ps -p "$pid" >/dev/null 2>&1; then
        echo "Still up after 15s, forcing."
        kill -9 "$pid" 2>/dev/null
    fi
    # The dev server spawns a reloader child that can outlive the parent.
    pkill -f "$APP_DIR/app.py" 2>/dev/null
    rm -f "$PID_FILE"
    echo "Stopped."
}

status() {
    local pid
    if pid="$(running_pid)"; then
        echo "Running (pid $pid) at $URL"
    elif [ -n "$(port_pid)" ]; then
        echo "Something is listening on port $PORT (pid $(port_pid)) but was not started by this script."
    else
        echo "Not running."
        return 1
    fi
    if command -v curl >/dev/null 2>&1; then
        local sync; sync="$(curl -fsS "$URL/api/sync/status" 2>/dev/null)"
        [ -n "$sync" ] && echo "Sync: $sync"
    fi
}

case "${1:-start}" in
    start)   start ;;
    stop)    stop ;;
    restart) stop; echo; start ;;
    status)  status ;;
    logs)    touch "$LOG_FILE"; tail -f "$LOG_FILE" ;;
    *)
        echo "Usage: $0 {start|stop|restart|status|logs}"
        exit 1
        ;;
esac
