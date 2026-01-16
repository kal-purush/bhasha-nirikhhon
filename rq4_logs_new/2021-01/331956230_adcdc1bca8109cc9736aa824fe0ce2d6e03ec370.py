import socket
import cv2
import numpy
import time
import sys
import json
import threading


def send_video():
    global frame_size
    global fps
    global encode_rate
    address = ('localhost', 8002)

    # estimate frame_size and fps
    capture = cv2.VideoCapture(0)
    frame_size_tot = 0
    start_time = int(round(time.time()))
    itr = 100  # iteration times set to 100, you can change it as you want
    # jpeg, 'encode_rate' for quality, higher the number, higher the quality(0-100)
    encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), encode_rate]
    for i in range(itr):
        ret, frame = capture.read()
        result, imgencode = cv2.imencode('.jpg', frame, encode_param)
        data = numpy.array(imgencode)
        frame_size_tot += data.size
    frame_size = frame_size_tot/itr/1000  # unit: KB
    time_interval = int(round(time.time())) - start_time
    fps = int(itr/time_interval)
    print("avg frame size is: " + str(frame_size) + "KB, fps is: " + str(fps))
    print("Initialization finished, you can start server now.")

    # waiting for the other thread
    while True:
        if isReady.is_set():
            break
        else:
            isReady.wait()

    try:
        # socket.SOCK_STREAM：for TCP
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(address)
    except socket.error as msg:
        print(msg)
        sys.exit(1)

    print("sending side: built connection with " + str(address))
    ret, frame = capture.read()   # ret == 1 for success, 0 for failure
    time_stamp = int(round(time.time() * 1000))  # record event time

    while ret:
        # time.sleep(0.01)  # you can put it to sleep in each loop to release the pressure of server
        reset_flag = False
        if needAdjust.is_set():  # need to adjust image quality to adapt to bandwidth
            encode_param[1] = int(encode_rate)
            reset_flag = True
            needAdjust.clear()
        result, imgencode = cv2.imencode('.jpg', frame, encode_param)
        # built mat
        data = numpy.array(imgencode)
        if reset_flag:  # update frame size
            frame_size = data.size
        string_data = data.tostring()
        # 'current_time': enable server to calculate bandwidth
        current_time = int(round(time.time() * 1000))
        body = {"length": len(string_data), "event_time": time_stamp, "current_time": current_time}
        body = json.dumps(body)
        sock.send(str.encode(str(len(body)).ljust(16)))
        sock.send(body.encode('utf-8'))
        sock.send(string_data)
        # then read next frame
        ret, frame = capture.read()
        time_stamp = int(round(time.time() * 1000))
        # if cv2.waitKey(10) == 27:
        #     break
    sock.close()


def receive_message():
    global encode_rate
    global frame_size
    global fps
    address = ('0.0.0.0', 8003)
    # socket.SOCK_STREAM：for TCP
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(address)
    # param: max connection number
    s.listen(1)

    def recvall(sock, count):
        buf = b''  # buf type: byte
        while count:
            newbuf = sock.recv(count)
            if not newbuf:
                return None
            buf += newbuf
            count -= len(newbuf)
        return buf

    conn, addr = s.accept()
    print('receiving side: accepted connection from '+str(addr))
    isReady.set()  # set event to inform the other thread
    up_count = down_count = 0
    while 1:
        length = int(recvall(conn, 16).rstrip())
        response = json.loads(recvall(conn, length))
        # compare current bandwidth and expected bandwidth
        average_bandwidth = response["bandwidth"]
        expected = fps * frame_size
        if average_bandwidth < expected - 100:
            down_count += 1
            if down_count > 5:
                encode_rate /= 2
                print("bandwidth is not enough, performing downgrade")
                needAdjust.set()
        if average_bandwidth > expected + 100 and encode_rate < 100:
            up_count += 1
            if up_count > 5:
                encode_rate = min(encode_rate + 10, 100)
                print("bandwidth is enough, performing upgrade")
                needAdjust.set()


if __name__ == '__main__':
    print("Please don't start server until the initialization finishes!")
    fps = 0
    frame_size = 0
    encode_rate = 95
    isReady = threading.Event()
    needAdjust = threading.Event()
    t1 = threading.Thread(target=send_video)
    t2 = threading.Thread(target=receive_message)
    t1.start()
    t2.start()