import socket

def send_message(message, receiver_ip, receiver_port):
    # Tạo socket UDP
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # Gửi thông báo đến địa chỉ IP và cổng nhận
    sock.sendto(message.encode(), (receiver_ip, receiver_port))

    # Đóng socket
    sock.close()
def send_notification(ip_list, message):
    # Thông tin máy nhận
    #ip_list = ["107.98.46.123", "107.98.46.111","107.98.46.91"]  # Địa chỉ IP của máy nhận
    try:
        receiver_port = 12345  # Cổng nhận trên máy nhận
        # Gửi thông báo
        for ip in ip_list:
            send_message(message, ip, receiver_port)
    except Exception as e:
        print(e)