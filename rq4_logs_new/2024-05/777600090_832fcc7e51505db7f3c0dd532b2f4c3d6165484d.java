package homework_nr_17;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class Constants {

    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_BLACK = "\u001B[30m";
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_BLUE = "\u001B[34m";
    public static final String ANSI_PURPLE = "\u001B[35m";
    public static final String ANSI_CYAN = "\u001B[36m";

    static List<String> colorList = new ArrayList<>(
            Arrays.asList(
                    ANSI_BLACK,
                    ANSI_BLUE,
                    ANSI_CYAN,
                    ANSI_RED,
                    ANSI_PURPLE,
                    ANSI_GREEN
            )
    );

    static Random random = new Random();

    public static String returnRandomColor(){
        return colorList.get(random.nextInt(0, 5));
    }
}