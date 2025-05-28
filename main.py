import os
import sys
import streamlit.web.cli as stcli


def resolve_path(path):
    """
    Resolve the absolute path of a file.
    """
    resolved_path = os.path.abspath(os.path.join(os.getcwd(), path))
    return resolved_path


def main():
    # Check if the script is running as a bundled executable
    if getattr(sys, 'frozen', False):
        # Running as a bundled executable
        bundle_dir = sys._MEIPASS  # PyInstaller creates this attribute
    else:
        # Running as a normal Python script
        bundle_dir = os.path.dirname(os.path.abspath(__file__))

    # Set the path to the main script
    script_path = os.path.join(bundle_dir, "Home.py")

    if not os.path.exists(script_path):
        raise FileNotFoundError(f"Could not find the script: {script_path}")

    # Set up sys.argv for Streamlit
    sys.argv = [
        "streamlit",
        "run",
        script_path,
        "--global.developmentMode=false",]

    # Run Streamlit
    sys.exit(stcli.main())


if __name__ == "__main__":
    main()