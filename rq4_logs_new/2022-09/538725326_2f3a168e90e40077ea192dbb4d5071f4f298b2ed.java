import com.roundsToThree.FileIO.DICOMLoader;
import com.roundsToThree.Structures.ValueRepresentation;
import com.roundsToThree.sd4j;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.Arrays;


public class TestSD4J {
    public static void main(String[] args) {

        // Load the DICOM slice
        try {
//            FileInputStream read = new FileInputStream(new File("000"));
            sd4j sd = sd4j.loadSlice(new File("000"));

        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}