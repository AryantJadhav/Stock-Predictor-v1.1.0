#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
#  ec2_setup.sh
#  Run ONCE on the EC2 Ubuntu server after deploying files from Windows.
#
#  Usage (after ssh-ing in):
#      bash /home/ubuntu/ec2_setup.sh
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail   # exit on any error, undefined var, or pipe failure

PROJECT_DIR="/home/ubuntu/Stock-Predictor"
TICK_DIR="$PROJECT_DIR/Tick_Data_Folder"
VENV_DIR="$PROJECT_DIR/venv"
SERVICE_SRC="$TICK_DIR/tick_harvester.service"
SERVICE_DST="/etc/systemd/system/tick_harvester.service"

echo ""
echo "══════════════════════════════════════════════════════════"
echo "   Stock Predictor — EC2 Server Setup"
echo "══════════════════════════════════════════════════════════"
echo ""

# ── 1. System packages ────────────────────────────────────────────────────────
echo "[ 1/7 ] Updating system packages …"
sudo apt-get update -y -q
sudo apt-get install -y -q python3 python3-venv python3-pip
echo "        Done."

# ── 2. Set timezone to IST ────────────────────────────────────────────────────
echo ""
echo "[ 2/7 ] Setting timezone to Asia/Kolkata (IST) …"
sudo timedatectl set-timezone Asia/Kolkata
echo "        $(timedatectl | grep 'Time zone')"

# ── 3. Python virtual environment ─────────────────────────────────────────────
echo ""
echo "[ 3/7 ] Creating Python virtual environment at $VENV_DIR …"
python3 -m venv "$VENV_DIR"
echo "        Done."

# ── 4. Install Python dependencies ────────────────────────────────────────────
echo ""
echo "[ 4/7 ] Installing requirements …"
"$VENV_DIR/bin/pip" install --upgrade pip -q
"$VENV_DIR/bin/pip" install -r "$TICK_DIR/requirements.txt"
echo "        Done."

# ── 5. Create tick_data output directory ─────────────────────────────────────
echo ""
echo "[ 5/7 ] Creating tick_data output directory …"
mkdir -p "$TICK_DIR/tick_data"
echo "        $TICK_DIR/tick_data"

# ── 6. Install systemd service ────────────────────────────────────────────────
echo ""
echo "[ 6/7 ] Installing systemd service …"
sudo cp "$SERVICE_SRC" "$SERVICE_DST"
sudo systemctl daemon-reload
sudo systemctl enable tick_harvester
echo "        Service enabled (will auto-start on boot)."

# ── 7. Verify installed packages ─────────────────────────────────────────────
echo ""
echo "[ 7/7 ] Verifying installed packages …"
"$VENV_DIR/bin/pip" show shareconnect websocket-client pycryptodome cryptography \
    | grep -E "^(Name|Version)"

# ── Final instructions ────────────────────────────────────────────────────────
echo ""
echo "══════════════════════════════════════════════════════════"
echo "  Setup complete!"
echo ""
echo "  NEXT STEP — Generate your first Sharekhan access token:"
echo ""
echo "    cd $TICK_DIR"
echo "    ../venv/bin/python auth_helper.py"
echo ""
echo "  Then start the harvester:"
echo ""
echo "    sudo systemctl start tick_harvester"
echo "    sudo journalctl -u tick_harvester -f"
echo "══════════════════════════════════════════════════════════"
echo ""
