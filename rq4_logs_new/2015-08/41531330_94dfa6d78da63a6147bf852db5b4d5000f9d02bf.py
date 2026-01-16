#In develop version
#TODO: checking available cams
#TODO: refactor
#TODO: maybe parsing line args

#First, check connected cams by running:
# $ ls /dev/video*

#To display stream, have to generate .sdp file, sample in config folder
#Data to .sdp file are displayed by running startStream command after SDP: statement


from time import sleep
import subprocess


class Streamer (object):
    """
    General class for streaming cams.
    """
    def __init__( self, rtp = '127.0.0.1', port = 8074 ):
        self.rtp_address = rtp
        self.port = port
        self.camera = 0
        self.input_resolution = "320x240"
        self.output_resolution = "1024x768"


    def startStream (self, camera = 0):
        """
        Starts stream with resolution of i/o specified in class fields.
        To change resolution see setResolution method.

        :param camera: number of cam from /dev/video*
        :return:
        """
        self.camera = camera
        self.runFF = subprocess.Popen( [    "ffmpeg",
                                            "-s", self.input_resolution,        #resolution
                                            "-i", "/dev/video" + str(camera),   #cam device number
                                            "-s", self.output_resolution,
                                            "-f", "rtp", "rtp://" + self.rtp_address + ":" + str(self.port) ],
                                            stdin = subprocess.PIPE )



    def DisplayStream(self):
        """
        Helper for test and debug purposes.
        """
        self.runVLC = subprocess.Popen( [   "vlc",
                                            "<absolute path to .sdp file with connection config>"])


    def stopStream (self) :
        """
        Stop stream and kills last streaming process.
        :return:
        """
        stop_output = self.runFF.kill()
        print stop_output

    def switchCamera (self, camera):
        """
        Change camera number, must be one of /dev/video*
        :param camera: number of cam
        :return:
        """
        self.runFF.kill()
        self.startStream(camera)


    def setInputStreamResolution (self, new_resolution):
        """
        Set new input resolution, will be applied until next change.
        :param new_resolution: New resolution in string format, ex: '640x420'
        """
        self.runFF.kill()
        self.input_resolution = new_resolution
        self.startStream(self.camera)


    def setOutputResolution (self, new_resolution):
        """
        :param new_resolution: New output resolution, will be applied when new stream will begin.
        """
        self.output_resolution = new_resolution

    def getStreamResolution (self):
        """
        Get actual resolution
        :return: resolution of stream
        """
        return self.resolution


    def __del__(self):
        self.runFF.kill()


#TEST
if __name__ == '__main__':
    testStreamer = Streamer()
    testStreamer.DisplayStream()
    testStreamer.startStream(1)

    sleep(10)

    testStreamer.switchCamera(2)

    sleep(10)

    testStreamer.switchCamera(3)

    sleep(10)

    testStreamer.setInputStreamResolution('128x76')

    sleep(10)

    testStreamer.stopStream()

    testStreamer.runVLC.kill()