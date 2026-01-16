package lukas.wais.aibooth.facerecognition;

import org.openimaj.image.DisplayUtilities;
import org.openimaj.image.FImage;
import org.openimaj.image.MBFImage;
import org.openimaj.image.colour.RGBColour;
import org.openimaj.image.colour.Transforms;
import org.openimaj.image.processing.edges.CannyEdgeDetector;
import org.openimaj.image.processing.face.detection.DetectedFace;
import org.openimaj.image.processing.face.detection.FaceDetector;
import org.openimaj.image.processing.face.detection.HaarCascadeDetector;
import org.openimaj.video.Video;
import org.openimaj.video.VideoDisplay;
import org.openimaj.video.VideoDisplayListener;
import org.openimaj.video.capture.VideoCapture;
import org.openimaj.video.capture.VideoCaptureException;

import java.util.List;

/**
 *
 * @author Lukas Wais
 */
public class FaceRecognition {
    Video<MBFImage> video;

    public FaceRecognition() {
        try {
            this.video = new VideoCapture(640,480);
        } catch (VideoCaptureException ex) {
            System.out.println("Error during video capture \n" + ex.getMessage());
        }
    }

    public Video<MBFImage> getVideo() {
        return video;
    }

    private void play() {
        VideoDisplay<MBFImage> display = VideoDisplay.createVideoDisplay(video);
        // Video listener
        display.addVideoListener(
                new VideoDisplayListener<>() {
                    public void beforeUpdate(MBFImage frame) {
                        /*
                            Use this for JavaFX
                            System.out.println(display.getScreen());
                            System.out.println(display.getDisplayFPS());
                        */
                        FaceDetector<DetectedFace, FImage> fd = new HaarCascadeDetector(40);
                        List<DetectedFace> faces = fd.detectFaces(Transforms.calculateIntensity(frame));
                        for (DetectedFace face : faces) {
                            frame.drawShape(face.getBounds(), RGBColour.RED);
                        }
                    }

                    public void afterUpdate(VideoDisplay<MBFImage> display) {
                    }
                });
    }

    public static void main(String[] args) {
        FaceRecognition application = new FaceRecognition();
        application.play();
    }
}

