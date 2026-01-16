package be.upgrade.it.adventofcode2020.day13;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ShuttleSearch {
    private final long start;
    private final String[] busses;

    public ShuttleSearch(long start, String[] busses) {
        this.start = start;
        this.busses = busses;
    }

    public long getWaitingTime(){
        long i = start;
        while (true){
            for (String bus : busses) {
                if("x".equals(bus)){
                    continue;
                }
                int j = Integer.parseInt(bus);
                if(i % j == 0){
                    return (i - start)  * j;
                }
            }
            i++;
        }
    }

    public long getGoldCoinTime(){
        List<Bus> buses = IntStream.range(0, busses.length)
                .filter(i -> !"x".equals(busses[i]))
                .mapToObj(i -> new Bus(Long.parseLong(busses[i]), Integer.valueOf(i).longValue()))
                .collect(Collectors.toList());

        long step = 1L;
        long t = 0L;

        for (Bus bus : buses) {
            while ((t + bus.getOffset()) % bus.getNumber() != 0) {
                t+=step;
            }
            step *= bus.getNumber();
        }

        return t;

    }

    private boolean check(long time, List<Bus> buses){
        for (Bus bus : buses) {
            if ((time - buses.get(0).getOffset() + bus.getOffset()) % bus.getNumber() != 0){
                return false;
            }
        }
        return true;
    }

    private static class Bus {
        private final long number;
        private final long offset;

        private Bus(long number, long offset) {
            this.number = number;
            this.offset = offset;
        }

        public long getNumber() {
            return number;
        }

        public long getOffset() {
            return offset;
        }
    }
}