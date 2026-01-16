import java.util.List;
import org.opencv.core.Core;
import org.opencv.core.Mat;
import org.opencv.core.MatOfRect;
import org.opencv.core.Point;
import org.opencv.core.Rect;
import org.opencv.core.Scalar;
import org.opencv.core.Size;
import org.opencv.highgui.HighGui;
import org.opencv.imgproc.Imgproc;
import org.opencv.objdetect.CascadeClassifier;
import org.opencv.videoio.VideoCapture;
	
class SignDetect{
	private static final int FRAME_HIEGHT = 1500;
	private static final int FRAME_WIDTH = 2000;
	private static final String CAMERA_NAME = "Capture Sign";
	
    public void detectAndDisplay(Mat frame, CascadeClassifier signCascade) {
        Mat frameGray = new Mat();
        Imgproc.cvtColor(frame, frameGray, Imgproc.COLOR_BGR2GRAY);
        Imgproc.equalizeHist(frameGray, frameGray);
        
        MatOfRect signs = new MatOfRect();
        signCascade.detectMultiScale(frameGray, signs);
        
        //Frames the identified sign
        List<Rect> listOfSigns = signs.toList();
        for(Rect sign : listOfSigns) { 
        	Point center = new Point(sign.x + sign.width/2, sign.y + sign.height/2);
        	int radius = (int) Math.round((sign.width + sign.height) * 0.25);
        	Imgproc.circle(frame, center, radius, new Scalar(255, 0, 0), 4);
        	
        	System.out.println("STOP SIGN");
        }
        
        //Resizes the frame to FRAMME_WIDTH & FRAME_HEIGHT
        Mat bigFrame = new Mat();
        Size sz = new Size(FRAME_WIDTH,FRAME_HIEGHT);
        Imgproc.resize(frame, bigFrame, sz);
        
        //Displays the camera footage and resizes GUI
        HighGui.namedWindow(CAMERA_NAME, HighGui.WINDOW_NORMAL);
        HighGui.resizeWindow(CAMERA_NAME, FRAME_WIDTH, FRAME_HIEGHT);
        HighGui.imshow(CAMERA_NAME, bigFrame );
        
    }
    
    public void run(String[] args) {
    	//Reads in classifier and camera from command prompt or using predetermined inputs
        String filenameSignsCascade = args.length > 2 ? args[1] : "C:/opencv/sources/data/haarcascades/stopsign_classifier.xml";
        int cameraDevice = args.length > 2 ? Integer.parseInt(args[2]) : 1;
        
        CascadeClassifier signCascade = new CascadeClassifier();
        
        //Prints and exits program if the classifier can not be found
        if (!signCascade.load(filenameSignsCascade)) {
            System.err.println("--(!)Error loading sign cascade: " + filenameSignsCascade);
            System.exit(0);
        }
       
        VideoCapture capture = new VideoCapture(cameraDevice);
        //VideoCapture captureStream = new VideoCapture();
       //captureStream.open(/*Video streaming link*/);
        
        //Exits program if camera can not be opened with videocapture
        if (!capture.isOpened()) {
            System.err.println("--(!)Error opening video capture");
            System.exit(0);
        }
        
        //Reads in frames from the videocapture until the capture stops
        Mat frame = new Mat();
        while (capture.read(frame)) {
            if (frame.empty()) {
                System.err.println("--(!) No captured frame -- Break!");
                break;
            }
            
            //Applies the classifier to the frame
            detectAndDisplay(frame, signCascade);
            
            if (HighGui.waitKey(10) == 27) {
                break;// escape
            }
        }
        System.exit(0);
    }
}

public class SignDetection {
    public static void main(String[] args) {
        // Load the native OpenCV library
        System.loadLibrary(Core.NATIVE_LIBRARY_NAME);
        new SignDetect().run(args);
    }
}