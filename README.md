# LAN Chat (Peer-to-Peer) — Đồ án Lập trình Socket

Ứng dụng chat P2P trên mạng LAN qua dòng lệnh (console), sử dụng **Native Python 3 Socket API** (`socket`, `threading`), bám sát 100% phạm vi đề bài `LAN_Chat_Socket_Project.docx.pdf`.

- **UDP (mặc định port `9999`)**: Phát quảng bá hiện diện mỗi `3 giây`, lắng nghe cập nhật danh sách online, xóa peer sau `10 giây` mất tín hiệu, hỗ trợ trạng thái `online` / `away`.
- **TCP (port qua `--port`, mặc định `5000`)**: Chat P2P giữa nhiều máy/instance bất kỳ lúc nào (lưu & báo `unread` cho peer không phải đối tác hiện tại) và truyền file qua luồng/kết nối TCP riêng lưu vào `downloads/` kèm tiến độ `%` (tự xóa file dở nếu ngắt kết nối).
- **Console UI (thuần thư viện chuẩn)**: Dùng khóa màn hình (`threading.Lock`) và ANSI redraw (`\r\033[K`) để tin nhắn/thông báo đến không làm vỡ dòng đang gõ trên cả Windows và macOS/Linux.

---

## 1. Yêu cầu môi trường & Cài đặt

- **Python**: Python 3.10 trở lên (không cần cài thêm thư viện ngoài; chạy 100% bằng thư viện chuẩn Python).
- **Tường lửa khi test 2+ máy LAN (Windows)**: Cho phép `python.exe` đi qua Windows Defender Firewall (mạng Private/Public) để nhận được gói UDP broadcast cổng `9999` và kết nối TCP.

---

## 2. Cách chạy ứng dụng

### Tham số khởi động (`lanchat.py`)

| Tham số | Bắt buộc | Mặc định | Mô tả |
| :--- | :---: | :--- | :--- |
| `--name` | Có | — | Tên hiển thị với các peer trong LAN |
| `--port` | Không | `5000` | Cổng TCP lắng nghe tin nhắn & file |
| `--udp-port` | Không | `9999` | Cổng UDP phát & nghe gói hiện diện `HELLO` |
| `--bcast` | Không | `255.255.255.255` | Địa chỉ UDP broadcast (dùng `127.255.255.255` khi test 1 máy) |

### Chạy test nhiều cửa sổ trên 1 máy

Mở 2–3 cửa sổ terminal khác nhau và gán cổng `--port` khác nhau:

```bash
# Terminal 1
python lanchat.py --name Quan --port 5001 --bcast 127.255.255.255

# Terminal 2
python lanchat.py --name Minh --port 5002 --bcast 127.255.255.255

# Terminal 3 (tùy chọn, test unread đa peer)
python lanchat.py --name Huy --port 5003 --bcast 127.255.255.255
```

### Chạy trên >= 2 máy thật cùng mạng LAN / Wi-Fi

```bash
# Máy A
python lanchat.py --name Quan --port 5000

# Máy B
python lanchat.py --name Minh --port 5000
```

---

## 3. Bảng 8 lệnh Console CLI

Mọi dòng không bắt đầu bằng `/` được gửi như tin nhắn chat tới đối tác hiện tại.

| Lệnh | Chức năng |
| :--- | :--- |
| `/list` | Hiển thị danh sách peer đang online (số thứ tự, tên, IP, trạng thái) |
| `/chat <name\|number>` | Mở kết nối TCP hoặc chuyển sang chat với peer theo tên hoặc số thứ tự trong `/list` |
| `<any text>` | Gửi một dòng tin nhắn tới đối tác đang chat hiện tại |
| `/send <file>` | Gửi file tới đối tác hiện tại trên luồng & kết nối TCP riêng (hiện `%`, không chặn chat) |
| `/leave` | Đóng phiên chat hiện tại (gửi `BYE\|`) |
| `/status <online\|away>` | Đổi trạng thái bản thân và phát quảng bá UDP ngay lập tức |
| `/help` | Hiển thị danh sách lệnh hỗ trợ |
| `/quit` | Thoát chương trình và đóng toàn bộ socket sạch sẽ |

---

## 4. Kiểm thử (Testing)

### Chạy toàn bộ unit test tự động

```bash
python -m unittest discover -s tests -v
python lanchat.py --help
```

### Kịch bản tự kiểm tra nhanh

1. **UDP Discovery & Timeout**: Bật 2 instance -> sau tối đa 3 giây gõ `/list` thấy nhau; gõ `/status away` thấy cập nhật; tắt 1 instance -> sau 10 giây bên kia báo offline và xóa khỏi `/list`.
2. **Chat đa peer & chống vỡ dòng**: Mở 3 instance A–B–C; đang gõ dở câu ở máy B (chưa bấm Enter) trong khi A/C gửi tin -> tin mới hiện phía trên, dòng đang gõ ở dấu nhắc `> ` vẫn nguyên vẹn.
3. **Truyền file & xóa file dở**: Gửi file vài MB bằng `/send <file>` trong khi vẫn chat qua lại bình thường; mở file nhận được trong `downloads/` để kiểm tra nguyên vẹn. Thử tắt nóng bên gửi giữa chừng -> xác nhận không còn file rác dở dang trong `downloads/`.

---

## 5. Cấu trúc dự án & Phân quyền file (Single Owner)

> **Quy tắc chống xung đột**: Mỗi người chỉ sửa file thuộc quyền sở hữu của mình. Khi cần đổi Interface trong `state.py` hoặc `protocol.py`, trao đổi để Nhóm trưởng cập nhật lên `main` trước rồi cả nhóm `git pull origin main`.

| File | Chủ sở hữu | Vai trò |
| :--- | :--- | :--- |
| `state.py` | **TV1 (Nhóm trưởng)** | `SharedState` được bảo vệ bởi `threading.Lock` |
| `protocol.py` | **TV1 (Nhóm trưởng)** | Framing UDP/TCP (`HELLO`, `PEER`, `MSG`, `FILE`, `BYE`) + `recv_exact` |
| `tcp_chat.py` | **TV1 (Nhóm trưởng)** | Luồng `TCP acceptor`, `TCP receiver`, nghiệp vụ `/chat` & `/leave` |
| `discovery.py` | **Thành viên 2** | Luồng `UDP broadcaster` (3s), `UDP listener` (10s timeout), `/list`, `/status` |
| `lanchat.py` | **TV2 (CLI) + TV1 (ghép)** | Điểm vào chương trình (`--name`, `--port`, `--udp-port`, `--bcast`) |
| `file_transfer.py` | **Thành viên 3** | Luồng `File sender` + nhận file vào `downloads/` (xóa partial file khi lỗi) |
| `console_ui.py` | **Thành viên 3** | Luồng `Main`, parser 8 lệnh mục 4.1, bảo vệ dòng đang gõ bằng `screen_lock` & ANSI redraw |
| `README.md` | **Thành viên 3** | Tài liệu hướng dẫn cài đặt, chạy 1 máy / LAN, bảng lệnh và kiểm thử |
| `tests/` | **Nhóm chung** | Bộ unit test cho `core`, `discovery`, `tcp_chat`, `console_ui` và `file_transfer` |

---

## 6. Quy trình làm việc với GitHub (Tối giản)

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
