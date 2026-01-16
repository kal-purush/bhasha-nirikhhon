package be.upgrade.it.adventofcode2020.day5;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Optional;

public class BoardingPass {
    private final String row;
    private final String column;

    public BoardingPass(final String input) {
        row = input.substring(0, 7);
        column = input.substring(7);
    }

    public final long getRow(){
        String binaryRow = row.replace('B', '1').replace('F', '0');
        long l = Long.parseLong(binaryRow, 2);
        return l;
    }

    public final long getColumn(){
        String binaryColumn = column.replace('R', '1').replace('L', '0');
        long l = Long.parseLong(binaryColumn, 2);
        return l;
    }

    public long getSeatId(){
        return getRow() * 8 + getColumn();
    }

    public final static long getHighestSeatId(String[] input){
        Optional<Long> optional = Arrays.stream(input)
                .map(s -> new BoardingPass(s).getSeatId())
                .sorted(Comparator.reverseOrder())
                .findFirst();

        return optional.orElse(0L);
    }
}