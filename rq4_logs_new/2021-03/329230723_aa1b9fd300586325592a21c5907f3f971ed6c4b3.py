class Camera:
    def __init__(self):
        #videos_information will contain the cv2 video capture object
        self.videos_information = None
        #frame will store the most recent frame of video captured by the video capture object
        self.frame = None
        #grabbed is a boolean that stores whether the frame has been grabbed or not
        self.grabbed = True
        #read_thread is the thread on which the particular camera is read
        self.read_thread = None
        #allows threads to be synchronized if different cameras are used
        self.read_lock = None
        #whether the program is still running or not
        self.running = True
    #This opens up the camera stream to allow frames to be analyzed through gstreamer
    def open(self, gstreamer_pipeline_string):
        try:
            self.video_capture = cv.VideoCapture(gstreamer_pipeline_string, cv2.CAP_GSTREAMER)
        except RuntimeError:
            self.video_capture = None
            print("Camera is not working at this point in time on the robot")
            
            self.grabbed, self.frame = self.video_capture.read()
    #This starts the camera stream and starts the thread that the camera will be streamed on        
    def start(self):
        if self.running:
            print("The video is capturing information and putting it on the interface")
            return None
        if self.video_capture != None:
            self.running = True
            self.read_thread = threading.Thread(target = self.updateCamera)
            self.read_thread.start()
        return self  
    #This updates the frames as they are inputted through the camera one by one          
    def updateCamera(self):
        while self.running:
            try:
                grabbed, frame = self.video_capture.read()
                with self.read_lock:
                    self.grabbed = grabbed
                    self.frame = frame
            except RuntimeError:
                print("Camera images are in the display but not on the actual interface")
    #This reads the actual frames put through by the video capture object and creates copies of the frames while locking the thread that the camera output is being shown on            
    def read(self):
        with self.read_lock:
            frame = self.frame.copy()
            grabbed = self.grabbed
            return frame, grabbed
    #This ends the camera stream and lets the camera stop outputting frames by setting the video capture object to none        
     def release(self):
         if self.video_capture!=None:
             self.video_capture.release()
             self.video_capture = None
         if self.read_thread != None:
             self.read_thread.join()     