package de.schalter.sermononline;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by martin on 02.02.18.
 */
public class UtilsTest {
    @Test
    public void fromString() throws Exception {

    }

    @Test
    public void toStringTest() throws Exception {

    }

    @Test
    public void indexOf() throws Exception {
        Integer[] array = {1, 2, 3, 4, 5};
        Integer[] contains = {7, 8, 9, 2};
        Integer[] containsNot = {};
        Integer[] containsNot2 = {6, 0};

        assertEquals(1, Utils.indexOf(array, contains));
        assertEquals(-1, Utils.indexOf(array, containsNot));
        assertEquals(-1, Utils.indexOf(array,containsNot2));
    }

    @Test
    public void indexOf1() throws Exception {
        List<Integer> array = new ArrayList<>();
        array.add(1);
        array.add(2);
        array.add(3);
        array.add(4);
        array.add(5);
        Integer[] contains = {7, 8, 9, 2};
        Integer[] containsNot = {};
        Integer[] containsNot2 = {6, 0};

        assertEquals(1, Utils.indexOf(array, contains));
        assertEquals(-1, Utils.indexOf(array, containsNot));
        assertEquals(-1, Utils.indexOf(array,containsNot2));
    }

    @Test
    public void getFileExtension() throws Exception {
        String noExtension = "test";
        String noExtension2 = "test.";
        String simpleExtension = "te%st.xml";
        String morePoints = "test.t/est.xml";
        String simpleUrl = "https://www.test.de/file.mp3";

        assertEquals(null, Utils.getFileExtension(noExtension));
        assertEquals(null, Utils.getFileExtension(noExtension2));
        assertEquals("xml", Utils.getFileExtension(simpleExtension));
        assertEquals("xml", Utils.getFileExtension(morePoints));
        assertEquals("mp3", Utils.getFileExtension(simpleUrl));
    }

    @Test
    public void toLowerCase() throws Exception {
        List<String> toConvert = new ArrayList<>();
        toConvert.add("Yeah");
        toConvert.add("Yeah:");
        toConvert.add("yeah");
        toConvert.add("yEaH;");

        List<String> expected = new ArrayList<>();
        expected.add("yeah");
        expected.add("yeah:");
        expected.add("yeah");
        expected.add("yeah;");

        List<String> converted = Utils.toLowerCase(toConvert);
        for(int i = 0; i < expected.size(); i++) {
            assertEquals(expected.get(i), converted.get(i));
        }
    }

    @Test
    public void convertDpToPixel() throws Exception {
    }

    @Test
    public void convertPixelsToDp() throws Exception {
    }

    @Test
    public void convertListToString() throws Exception {
    }

    @Test
    public void convertListToString1() throws Exception {
    }

    @Test
    public void convertStringToList() throws Exception {
    }

    @Test
    public void convertStringToList1() throws Exception {
    }

}