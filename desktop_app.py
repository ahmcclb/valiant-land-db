# desktop_app.py
import webview
import threading
import sys
import os
import time
import json
import shutil
import base64
from pathlib import Path

# Add current directory to path for imports
if getattr(sys, 'frozen', False):
    base_dir = os.path.dirname(sys.executable)
else:
    base_dir = os.path.dirname(os.path.abspath(__file__))

sys.path.insert(0, base_dir)

class JSApi:
    def download_file(self, filename):
        """
        Called from JavaScript to save a file from static/exports/
        Returns: {'success': bool, 'path': str or None, 'cancelled': bool, 'error': str}
        """
        try:
            source_path = os.path.join(base_dir, 'static', 'exports', filename)
            
            print(f"[DEBUG] Attempting to download: {source_path}")
            print(f"[DEBUG] File exists: {os.path.exists(source_path)}")
            
            if not os.path.exists(source_path):
                error_msg = f'File not found: {filename}'
                print(f"[ERROR] {error_msg}")
                return {'success': False, 'error': error_msg, 'cancelled': False}
            
            if not webview.windows:
                return {'success': False, 'error': 'Window not available', 'cancelled': False}
            
            window = webview.windows[0]
            
            # create_file_dialog returns a tuple (file_path,) or empty tuple
            result = window.create_file_dialog(
                webview.SAVE_DIALOG,
                directory='',
                save_filename=filename
            )
            
            print(f"[DEBUG] Dialog result: {result}")
            
            if result and len(result) > 0:
                # CRITICAL FIX: result is always a tuple, take first element
                dest_path = result[0]
                print(f"[DEBUG] Copying to: {dest_path}")
                
                try:
                    shutil.copy2(source_path, dest_path)
                    print(f"[DEBUG] Copy successful")
                    return {'success': True, 'path': dest_path, 'cancelled': False}
                except Exception as copy_error:
                    error_msg = f'Copy failed: {str(copy_error)}'
                    print(f"[ERROR] {error_msg}")
                    return {'success': False, 'error': error_msg, 'cancelled': False}
            else:
                print("[DEBUG] User cancelled dialog")
                return {'success': False, 'cancelled': True}
                
        except Exception as e:
            error_msg = f"Exception: {str(e)}"
            print(error_msg)
            import traceback
            traceback.print_exc()
            return {'success': False, 'error': error_msg, 'cancelled': False}

    def save_download_file(self, filename, data_url):
        """
        Save file from data URL (base64)
        Data URL format: data:application/octet-stream;base64,SGVsbG8...
        """
        try:
            print(f"[DEBUG] save_download_file called for: {filename}")
            
            if ',' not in data_url:
                return {'success': False, 'error': 'Invalid data URL format', 'cancelled': False}
                
            header, base64_data = data_url.split(',', 1)
            print(f"[DEBUG] Header: {header}")
            
            try:
                file_data = base64.b64decode(base64_data)
                print(f"[DEBUG] Decoded {len(file_data)} bytes")
            except Exception as decode_error:
                return {'success': False, 'error': f'Decode error: {str(decode_error)}', 'cancelled': False}
            
            if not webview.windows:
                return {'success': False, 'error': 'Window not available', 'cancelled': False}
            
            window = webview.windows[0]
            
            # CRITICAL FIX: create_file_dialog returns tuple
            result = window.create_file_dialog(
                webview.SAVE_DIALOG,
                directory='',
                save_filename=filename
            )
            
            print(f"[DEBUG] Save dialog result: {result}")
            
            if result and len(result) > 0:
                # CRITICAL FIX: result is always a tuple, take first element
                dest_path = result[0]
                print(f"[DEBUG] Writing to: {dest_path}")
                
                try:
                    with open(dest_path, 'wb') as f:
                        f.write(file_data)
                    print(f"[DEBUG] Write successful")
                    return {'success': True, 'path': dest_path, 'cancelled': False}
                except Exception as write_error:
                    error_msg = f'Write failed: {str(write_error)}'
                    print(f"[ERROR] {error_msg}")
                    return {'success': False, 'error': error_msg, 'cancelled': False}
            else:
                print("[DEBUG] User cancelled")
                return {'success': False, 'cancelled': True}
                
        except Exception as e:
            error_msg = f"Exception: {str(e)}"
            print(error_msg)
            import traceback
            traceback.print_exc()
            return {'success': False, 'error': error_msg, 'cancelled': False}

# Create API instance - note: we don't set window here to avoid recursion
api = JSApi()

from app import app
from waitress import serve
import database_pg

# Global state
shutdown_event = threading.Event()
server_ready = threading.Event()

def get_config_path():
    """Get config path - external to executable for easy editing"""
    if getattr(sys, 'frozen', False):
        # Running as compiled .exe - look in same folder
        return os.path.join(os.path.dirname(sys.executable), 'config.json')
    else:
        # Running as script
        return os.path.join(base_dir, 'config.json')

def verify_environment():
    """Verify that static directories exist and are writable"""
    print("[DEBUG] Environment Verification:")
    print(f"  Frozen: {getattr(sys, 'frozen', False)}")
    print(f"  Executable: {sys.executable}")
    print(f"  Base Dir: {base_dir}")
    
    test_dirs = [
        os.path.join(base_dir, 'static'),
        os.path.join(base_dir, 'static', 'uploads'),
        os.path.join(base_dir, 'static', 'uploads', 'photos'),
        os.path.join(base_dir, 'static', 'uploads', 'documents'),
        os.path.join(base_dir, 'static', 'exports')
    ]
    
    for d in test_dirs:
        exists = os.path.exists(d)
        writable = os.access(d, os.W_OK) if exists else False
        print(f"  {d}: exists={exists}, writable={writable}")
        
        # Create if missing
        if not exists:
            try:
                os.makedirs(d, exist_ok=True)
                print(f"    -> Created")
            except Exception as e:
                print(f"    -> ERROR creating: {e}")

def ensure_directories():
    """Create upload directories if they don't exist"""
    dirs = [
        'static/uploads/photos',
        'static/uploads/documents', 
        'static/exports'
    ]
    for dir_path in dirs:
        full_path = os.path.join(base_dir, dir_path)
        os.makedirs(full_path, exist_ok=True)
        print(f"[DEBUG] Ensured directory: {full_path}")

def check_sync_status():
    """Check if sync is currently running via sync_service"""
    try:
        import sync_service
        # Check if there's an active sync instance running
        if hasattr(sync_service, 'sync_instance') and sync_service.sync_instance:
            return sync_service.sync_instance.is_syncing
        return False
    except:
        return False

def run_server():
    """Run Waitress WSGI server in background thread"""
    try:
        # Use 0.0.0.0 to allow local access but bind to localhost for security
        print("[DEBUG] Starting Flask server on 127.0.0.1:5000")
        serve(app, host='127.0.0.1', port=5000, threads=6, _quiet=True)
    except Exception as e:
        print(f"[ERROR] Server error: {e}")

def on_closing():
    """
    Called when user clicks X button or selects Exit
    Returns False to prevent closing, True to allow
    """
    print("[DEBUG] on_closing called")
    
    # Check if sync is in progress
    if check_sync_status():
        print("[DEBUG] Sync in progress, asking user")
        try:
            if webview.windows:
                window = webview.windows[0]
                # Use JavaScript confirm dialog instead of Python dialog (which doesn't exist in pywebview)
                result = window.evaluate_js("confirm('Database synchronization is currently running. Closing now may cause incomplete data transfer. Click OK to close anyway, or Cancel to wait.')")
                print(f"[DEBUG] User choice: {result}")
                if not result:  # User clicked Cancel
                    return False  # Don't close
        except Exception as e:
            print(f"[ERROR] Error showing confirm dialog: {e}")
            # If we can't show dialog, prevent closing to be safe
            return False
    
    # Clean shutdown sequence
    print("Shutting down gracefully...")
    
    # Close database connections
    try:
        database_pg.close_all_connections()
    except:
        pass
    
    # Signal shutdown
    shutdown_event.set()
    
    # Give server a moment to finish current requests
    time.sleep(0.5)
    
    return True  # Allow window to close

def show_about():
    """Menu callback for Help > About"""
    try:
        if webview.windows:
            window = webview.windows[0]
            window.evaluate_js("alert('Valiant Land Real Estate Database\\nVersion 1.0\\n\\nLocal Database: PostgreSQL\\nCloud Sync: Supabase\\n© 2026 Valiant Land')")
    except Exception as e:
        print(f"[ERROR] Error showing about: {e}")

if __name__ == '__main__':
    # Ensure config exists
    config_path = get_config_path()
    if not os.path.exists(config_path):
        print(f"[ERROR] Configuration missing: {config_path}")
        # Try to show error using simple message box if possible, otherwise print
        sys.exit(1)
        
    # Add this line before starting server:
    verify_environment()
    
    # Ensure upload directories exist
    ensure_directories()
    
    # Start Flask server in background
    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.start()
    
    # Wait a moment for server to start
    time.sleep(1.5)
    
    # Create window
    window = webview.create_window(
        'Valiant Land Database',
        'http://127.0.0.1:5000',
        width=1600,
        height=1000,
        min_size=(1400, 800),
        resizable=True,
        text_select=True,
        js_api=api,  # Expose API - note: api does NOT contain window reference
        confirm_close=False
    )
        
    window.events.loaded += lambda: window.maximize()
    
    # Create menu
    menu_items = [
        webview.menu.Menu('File', [
            webview.menu.MenuAction('Exit', lambda: window.destroy())
        ]),
        webview.menu.Menu('Help', [
            webview.menu.MenuAction('About', show_about)
        ])
    ]
    window.menu = menu_items
    
    # Bind close event
    window.events.closing += on_closing
    
    # Start GUI (blocks until window closes)
    print("[DEBUG] Starting webview")
    webview.start(gui='edge', debug=False)
    
    # Cleanup after window closes
    print("[DEBUG] Exiting")
    sys.exit(0)