"""
DarkFleet-IQ Application Launcher
=================================
Runs the interactive Streamlit forensics platform locally and opens the browser.
"""

import os
import sys
import subprocess

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
APP_PATH = os.path.join(BASE_DIR, "app.py")

def main():
    print("=" * 80)
    print("  🛰️ DarkFleet-IQ: Interactive Maritime Telemetry & Forensics Platform")
    print("=" * 80)
    print(f"Launching Streamlit application from: {APP_PATH}")
    print("Press Ctrl+C in this terminal to stop the server.\n")
    
    cmd = [
        sys.executable, "-m", "streamlit", "run", APP_PATH,
        "--server.headless=false",
        "--browser.gatherUsageStats=false"
    ]
    
    try:
        subprocess.run(cmd, cwd=BASE_DIR)
    except KeyboardInterrupt:
        print("\n[INFO] Server stopped gracefully.")

if __name__ == "__main__":
    main()
