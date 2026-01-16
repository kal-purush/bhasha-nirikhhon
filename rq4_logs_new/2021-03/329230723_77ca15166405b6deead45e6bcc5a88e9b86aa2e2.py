if __name__ == '__main__':
    camera = Camera()
    interface = Interface()
    controller = control(100,0.1)
    interface.make_interface() 
    try:
        controls_move()
        print("Controller is working")
    except KeyboardInterrupt:
        print("Stop")
    except:
        print("Unexpected error: ", sys.exc_infor()[0])
        raise
    if(not camera.video_capture.isOpened()):
        print("Start Cameras")
        SystemExit(0)
    while self.running:
        _, camera_picture = camera.read()
        camera_pictures = np.hstack((camera_picture))
        interface.update_interface(cameraFrameSize=(600,600), graphFrameSize=(600,600),videoFrames=camera_pictures)

        camera.stop()
        camera.release()
    
    
'''