# =============================================================================
# LOG SERVER CHO SPARK DSTREAMS DEMO
# Mục đích: Gửi log ngẫu nhiên qua socket TCP để Spark DStreams xử lý
# Cách hoạt động: Đọc file sample_logs.txt → Thêm timestamp → Gửi cho Spark
# =============================================================================

import socket          # Kết nối mạng
import time            # Delay giữa các log
import random          # Chọn log ngẫu nhiên
import threading       # Xử lý nhiều kết nối
from pathlib import Path  # Đọc file
from datetime import datetime  # Tạo timestamp

# Cấu hình server
HOST = "127.0.0.1"     # Địa chỉ localhost
PORT = 9999            # Cổng để Spark kết nối
INTERVAL_SEC = 2       # Gửi log mỗi 2 giây

# Đọc tất cả log mẫu từ file
SAMPLES = Path("sample_logs_exercise.txt").read_text(encoding="utf-8", errors="ignore").splitlines()
print(f"Đã đọc {len(SAMPLES)} dòng log mẫu từ sample_logs_exercise.txt")

def handle_client(conn, addr):
    """Xử lý khi Spark kết nối: gửi log liên tục"""
    print(f"Spark DStreams connected from {addr}")
    print(f"Bắt đầu streaming logs mỗi {INTERVAL_SEC}s...")
    
    try:
        count = 0
        while True:
            # Chọn 1 log ngẫu nhiên
            original_msg = random.choice(SAMPLES)
            
            # Thêm timestamp hiện tại
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            msg = f"{timestamp} - {original_msg}"
            
            # Gửi cho Spark
            conn.sendall((msg + "\n").encode("utf-8"))
            
            # Hiển thị log đã gửi
            count += 1
            print(f"[{count}] {msg}")
            
            # Dừng 2 giây trước khi gửi tiếp
            time.sleep(INTERVAL_SEC)
    except (BrokenPipeError, ConnectionResetError):
        print(f"Spark client {addr} disconnected.")
    finally:
        conn.close()
        print(f"Connection to {addr} closed.")

def main():
    """Khởi tạo server và lắng nghe kết nối từ Spark"""
    print("LOG SERVER FOR SPARK DSTREAMS DEMO")
    print("=" * 40)
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Tái sử dụng địa chỉ
        s.bind((HOST, PORT))                                      # Gắn địa chỉ
        s.listen(1)                                              # Lắng nghe 1 kết nối
        
        print(f"Log server listening on {HOST}:{PORT}")
        print("Waiting for Spark DStreams connection...")
        print("Run log_analyzer_exercise.py to connect")
        
        while True:
            conn, addr = s.accept()                              # Chấp nhận kết nối
            # Tạo thread xử lý client
            threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()

if __name__ == "__main__":
    """Chạy chương trình"""
    main()
