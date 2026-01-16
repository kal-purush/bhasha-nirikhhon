/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package camcap;

import org.opencv.core.*;
import org.opencv.imgcodecs.Imgcodecs;

import java.awt.Image;
import org.opencv.features2d.Features2d;
import org.opencv.videoio.*;

public class CamCap_2 {

    public static VideoCapture capture = null;
    public static Viewer window = null;
    public static CamExecutor preExecute = null;

    public static void FindRobot(Image img, Mat I) {
        CamExecutor cam_exec = new CamExecutor();
//        cam_exec.Execute(frame, I);
        cam_exec.det_feat_scene = preExecute.det_feat_scene;
        cam_exec.keypoints_scene = preExecute.keypoints_scene;
        cam_exec.ExecuteObject(I);
        cam_exec.ExecuteMatch();
        System.out.println("Keypoints found: " + cam_exec.keypoints_scene.toList().size());
        cam_exec.FindGood();
        //cam_exec.DeleteOdds();
        MatOfPoint2f obj = cam_exec.FindObject();
        MatOfPoint2f scene = cam_exec.FindScene();
//        Mat frame_temp = new Mat();
//        MatOfByte drawnMatches = new MatOfByte();
//        Features2d.drawMatches(frame, cam_exec.keypoints_scene, I, cam_exec.keypoints_object,
//                cam_exec.good_match_mat, frame_temp, new Scalar(255, 0, 0),
//                new Scalar(0, 0, 255), drawnMatches, Features2d.NOT_DRAW_SINGLE_POINTS);
//
//        Image img = Viewer.toBufferedImage(frame_temp);
        
//        Image img = Viewer.toBufferedImage(frame);

        try {
            HomographyFinder hom_fin = new HomographyFinder();
            Mat H = hom_fin.FindHom(obj, scene);

            double[][] var = hom_fin.FindBorders(H, I);
            window.DrawHomography(img, var);
            System.out.println("Homography found");
        } catch (Exception e) {
            System.err.println("Fail to make homography " + e);
        }
    }

    public static void main(String[] args) {
        System.loadLibrary(Core.NATIVE_LIBRARY_NAME);
        capture = new VideoCapture(0);
        capture.set(Videoio.CV_CAP_PROP_FRAME_HEIGHT, 480);
        capture.set(Videoio.CV_CAP_PROP_FRAME_WIDTH, 640);
        if (!capture.isOpened()) {
            System.out.println("Error");
        } else {
            Mat frame = new Mat();

            window = new Viewer();
            while (window.window.isVisible() == true) {

                long time1 = System.nanoTime();
                capture.read(frame);
                frame = Viewer.convertToGray(frame);

                Mat I = Imgcodecs.imread("data/test3.png");
                if (I.empty()) {
                    System.err.println("Error opening image");
                }
                I = Viewer.convertToGray(I);

                Mat J = Imgcodecs.imread("data/test5.png");
                if (I.empty()) {
                    System.err.println("Error opening image");
                }
                J = Viewer.convertToGray(J);

                preExecute = new CamExecutor();
                preExecute.ExecuteScene(frame);
                Image img = Viewer.toBufferedImage(frame);

                FindRobot(img, I);
                FindRobot(img, J);
                
//                Image img = Viewer.toBufferedImage(preFrame);
                window.Repaint(img);

                long time2 = System.nanoTime();
                System.out.println("FPS = " + 1 / ((time2 - time1) / 1e9));
                if (window.window.isVisible() == false) {
                    capture.release();
                    break;
                }
            }
        }
//        capture.release();
    }
}