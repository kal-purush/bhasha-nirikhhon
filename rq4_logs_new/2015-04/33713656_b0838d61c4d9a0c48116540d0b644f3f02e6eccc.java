import org.wiigee.device.Device;
import org.wiigee.event.ButtonPressedEvent;
import java.util.Random;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class test2{

    static Device device;

    static int trainButton;
    static int closeGestureButton;
    static int recognitionButton;

    public static void train_gesture(int gesture_id, String dir, int n_gestures)
        throws Exception{
        String filename;
        String line;
        String[] data;
        BufferedReader br;
        double[] event;
        for (int i = 0; i < n_gestures; i++){
            filename = dir + i + ".csv";
            br = new BufferedReader(new FileReader(filename));
            line = br.readLine(); //discard

            device.fireButtonPressedEvent(trainButton);//start gesture

            while ((line = br.readLine()) != null){
                data = line.split(",");
                event = new double[3];
                for (int j = 0; j < 3 ; j++){
                    event[j] = Double.parseDouble(data[j+1]);
                }
                device.fireAccelerationEvent(event);
            }
            device.fireButtonReleasedEvent(trainButton);//stop gesture
        }

        //signal end of gesture training
        device.fireButtonPressedEvent(closeGestureButton);
        device.fireButtonReleasedEvent(closeGestureButton);
    }


    public static void recognize_gesture(String filename)
        throws Exception{
        String line;
        String[] data;
        BufferedReader br;
        double[] event;

        br = new BufferedReader(new FileReader(filename));
        line = br.readLine(); //discard

        device.fireButtonPressedEvent(recognitionButton);//start gesture

        while ((line = br.readLine()) != null){
            data = line.split(",");
            event = new double[3];
            for (int j = 0; j < 3 ; j++){
                event[j] = Double.parseDouble(data[j+1]);
            }
            device.fireAccelerationEvent(event);
        }
        device.fireButtonReleasedEvent(recognitionButton);//stop gesture
    }

    public static void main(String[] args) throws Exception{
        //WiimoteWiigee wiigee = new WiimoteWiigee();

        device = new Device(true); //autofiltering=true

        trainButton = ButtonPressedEvent.BUTTON_A;
        closeGestureButton = ButtonPressedEvent.BUTTON_HOME;
        recognitionButton = ButtonPressedEvent.BUTTON_B;

        device.setTrainButton(trainButton);
        device.setCloseGestureButton(closeGestureButton);
        device.setRecognitionButton(recognitionButton);

        //train gestures
        train_gesture(0, "gesture_recordings/circle/", 14);
        train_gesture(1, "gesture_recordings/square/", 14);
        train_gesture(2, "gesture_recordings/z/", 14);
        train_gesture(3, "gesture_recordings/back_and_forth/", 14);
        train_gesture(4, "gesture_recordings/roll_flip/", 14);

        //save models
        device.saveGesture(0, "gesture_recordings/circle/model");
        device.saveGesture(1, "gesture_recordings/square/model");
        device.saveGesture(2, "gesture_recordings/z/model");
        device.saveGesture(3, "gesture_recordings/back_and_forth/model");
        device.saveGesture(4, "gesture_recordings/roll_flip/model");

        //test recognition
        recognize_gesture("gesture_recordings/circle/14.csv");
        recognize_gesture("gesture_recordings/square/14.csv");
        recognize_gesture("gesture_recordings/z/14.csv");
        recognize_gesture("gesture_recordings/back_and_forth/14.csv");
        recognize_gesture("gesture_recordings/roll_flip/14.csv");
    }
}