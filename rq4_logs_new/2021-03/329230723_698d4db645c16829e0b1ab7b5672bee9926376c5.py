class interface():
    def __init__(self, robot, object_seen, motor_controls):
        self.robot = None
        self.object_seen = True
        self.motor_controls = True
    #make_interface makes the user interface that will use information parsed through from the camera and the controls in order to make information about the robot's speed, acceleration, and other measurements available

    def make_interface(self):
        self.camera_layout = [[sg.Text(title="Camera", size=(60,60), justification="center")], [sg.Image(filename="", key="camera")]]
        self.camera_column = sg.Column(camera_layout, element_justification="center")
        self.speed_graph_layout = [[sg.Text("Speeds Graphs", size=(60,60), justification="center")], [sg.Image(filename="graph.png", key="graphs")]]
        self.layout = [camera_layout, speed_graph_layout]
        self.window = sg.Window(title="Interface", layout=layout, return_keyboard_events=True,location=(600,600))
    def update_interface(self, cameraFrameSize, graphFrameSize, videoFrames):
        self.running = True
        self.cameraFrameSize = cameraFrameSize
        self.graphFrameSize = graphFrameSize
        while self.running:
            event, values = window.read()
            if event == sg.WIN_CLOSED or event == "EXIT":
                self.running = False
            frame = cv2.resize(videoFrames, self.graphFrameSize)
            frame_grayversion = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            images_bytess = cv2.imencode(".png", frame_grayversion)[1].tobytes()
            window["camera"].update(data=images_bytess)