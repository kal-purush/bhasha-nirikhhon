import socket    
from flask import Flask, render_template ,Response
from video_Streaming import Video
app=Flask(__name__)

def getIp():
    s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    try:
        s.connect(('10.255.255.255',1))
        IP=s.getsockname()[0]
    except :
        hostname=socket.gethostname()
        IP=socket.gethostbyname(hostname)
    finally:
        s.close()

    return IP


@app.route('/')
def index():
    return render_template('index.html')

def Streaming(camera):
    while True:
        frame=camera.get_frame()
        yield(b'--frame\r\n'
       b'Content-Type:  image/jpeg\r\n\r\n' + frame +
         b'\r\n\r\n')

@app.route('/video')
def video():
    return Response(Streaming(Video()),
    mimetype='multipart/x-mixed-replace; boundary=frame')

    


if __name__=='__main__':
    app.run(host=getIp(),port='5000',debug=True)
