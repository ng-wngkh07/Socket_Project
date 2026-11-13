# LAN Chat (Peer-to-Peer) — Đồ án Lập trình Socket

Ứng dụng chat P2P trên mạng LAN qua dòng lệnh (console), sử dụng **Native Python 3 Socket API** (`socket`, `threading`), bám sát 100% phạm vi đề bài `LAN_Chat_Socket_Project.docx.pdf`.

- **UDP (mặc định port `9999`)**: Phát quảng bá hiện diện mỗi `3 giây`, lắng nghe cập nhật danh sách online, xóa peer sau `10 giây` mất tín hiệu, hỗ trợ trạng thái `online` / `away`.
- **TCP (port qua `--port`, mặc định `5000`)**: Chat P2P giữa nhiều máy/instance bất kỳ lúc nào (lưu & báo `unread` cho peer không phải đối tác hiện tại) và truyền file lưu vào `downloads/` kèm tiến độ `%`.

---

## Cấu trúc dự án & Phân quyền file (Single Owner)

> **Quy tắc chống xung đột**: Mỗi người chỉ sửa file thuộc quyền sở hữu của mình. Khi cần đổi Interface trong `state.py` hoặc `protocol.py`, trao đổi để Nhóm trưởng cập nhật lên `main` trước rồi cả nhóm `git pull origin main`.

| File | Chủ sở hữu | Vai trò |
| :--- | :--- | :--- |
| `state.py` | **TV1 (Nhóm trưởng)** | `SharedState` được bảo vệ bởi `threading.Lock` (đã cài sẵn) |
| `protocol.py` | **TV1 (Nhóm trưởng)** | Framing UDP/TCP (`HELLO`, `PEER`, `MSG`, `FILE`, `BYE`) + `recv_exact` (đã cài sẵn) |
| `tcp_chat.py` | **TV1 (Nhóm trưởng)** | Luồng `TCP acceptor`, `TCP receiver`, nghiệp vụ `/chat` & `/leave` |
| `discovery.py` | **Thành viên 2** | Luồng `UDP broadcaster` (3s), `UDP listener` (10s timeout), `/list`, `/status` |
| `lanchat.py` | **TV2 (CLI) + TV1 (ghép)** | Điểm vào chương trình (`--name`, `--port`, `--udp-port`, `--bcast`) |
| `file_transfer.py` | **Thành viên 3** | Luồng `File sender` + nhận file vào `downloads/` (xóa partial file khi lỗi) |
| `console_ui.py` | **Thành viên 3** | Luồng `Main`, parser 8 lệnh mục 4.1, bảo vệ dòng đang gõ (`\r\033[K` / `prompt_toolkit`) |
| `tests/test_core.py` | **Nhóm chung** | Kiểm thử unit cho `state.py`, `protocol.py` và kiểm tra import toàn dự án |

---

## Quy trình làm việc với GitHub (Tối giản)

- `main`: Nhánh ổn định, luôn chạy được. Chỉ TV1 merge Pull Request sau khi kiểm tra.
- 3 nhánh làm việc riêng: `tv1-tcp-core`, `tv2-udp-discovery`, `tv3-ui-file`.

```bash
# 1. Lấy code mới nhất từ main trước khi làm việc
git checkout <ten-nhanh-cua-ban>
git pull origin main

# 2. Sau khi tự test xong phần mình sở hữu
git add <cac-file-cua-ban>
git commit -m "feat(<ten-module>): mo ta ngan gon"
git push -u origin <ten-nhanh-cua-ban>
```

---

## Yêu cầu & Kiểm thử

- **Môi trường**: Python 3.10+ (thư viện chuẩn). Nếu TV3 chọn `prompt_toolkit` cho Console UI thì chạy `pip install -r requirements.txt`.
- **Chạy kiểm thử phần lõi có sẵn**:

```bash
python3 -m unittest discover -s tests -v
python3 lanchat.py --help
```

- **Chạy thử nghiệm ứng dụng (sau khi hoàn thành các module M2–M4)**:

```bash
# Test 2 instance trên cùng 1 máy:
python3 lanchat.py --name Quan --port 5001 --bcast 127.255.255.255
python3 lanchat.py --name Minh --port 5002 --bcast 127.255.255.255

# Test trên >=2 máy cùng mạng LAN / Wi-Fi:
python3 lanchat.py --name Quan --port 5000
```
