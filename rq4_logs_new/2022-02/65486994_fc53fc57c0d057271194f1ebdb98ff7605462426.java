package nl.bos.a2019;

import nl.bos.general.AdventReadInput;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;

public class Day2ProgramAlarm {

    private final long sum = 0;

    public Day2ProgramAlarm() {
        InputStream is = getClass().getClassLoader().getResourceAsStream("nl/bos/a2019/Day2ProgramAlarm");
        ArrayList<String> intCode = AdventReadInput.readData(is);
        int[] integers = Arrays.stream(intCode.get(0).split(","))
                .mapToInt(Integer::parseInt)
                .toArray();
        int opcode = integers[0];

        System.out.println(0);
    }

    public static void main(String[] args) {
        new Day2ProgramAlarm();
    }

}