package Cammons;

import LetterPackage.PlacedLetter;
import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;

/**
 *
 * @author Jirka
 */
public class Cammons {

    /**
     * Creates a new instance of Cammons
     */
    public Cammons() {
    }

    static final public String extractFilePath(String s) {

        int i = s.lastIndexOf(File.separator);
        return s.substring(0, i + 1);

    }

    static final public String extractFileName(String s) {

        int i = s.lastIndexOf(File.separator);
        return s.substring(i + 1, s.length());

    }

    public static final String deleteSuffix(String file) {
        int x = file.length();
        while (x >= 0) {
            x--;
            if (file.charAt(x) == '.') {
                return file.substring(0, x);

            }
            if (file.charAt(x) == File.separatorChar) {
                return file;
            }
        }
        return null;
    }

    static final double vdalenost2d(double x, double y, double x2, double y2) {

        return Math.sqrt((x - x2) * (x - x2) + (y - y2) * (y - y2));
    }

    public static final String vytvorZpravuOTahu(ArrayList<PlacedLetter> a) {
        String s = "";
        for (PlacedLetter elem : a) {
            s = s + "-" + elem.toString();
        }
        return s;
    }

}