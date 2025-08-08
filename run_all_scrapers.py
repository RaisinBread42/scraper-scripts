import subprocess
import glob
import os
os.environ["PYTHONIOENCODING"] = "utf-8"

import sys
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

def main():
    root_dir = os.path.dirname(os.path.abspath(__file__))
    py_files = [
        f for f in glob.glob(os.path.join(root_dir, "*.py"))
        if os.path.basename(f) != os.path.basename(__file__)
    ]

    for py_file in py_files:
        print(f"\nRunning: {os.path.basename(py_file)}")
        try:
            result = subprocess.run(
                [sys.executable, py_file],
                capture_output=True,
                text=True,
                timeout=600,
                encoding="utf-8"  # <-- Add this line
            )
            print(result.stdout)
            if result.stderr:
                print("Errors:", result.stderr)
        except subprocess.TimeoutExpired:
            print(f"Timeout: {os.path.basename(py_file)} took too long and was terminated.")

if __name__ == "__main__":
    main()