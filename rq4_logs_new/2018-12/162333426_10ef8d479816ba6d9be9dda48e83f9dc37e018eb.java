package adventofcode.day4;

import adventofcode.common.FileUtil;

import java.util.*;
import java.util.stream.Collectors;

public class Day4 {

    public Day4() {
        try {
            List<String> input = FileUtil.readInputFile(this.getClass());
            System.out.println(solvePart1(input));
            System.out.println(solvePart2(input));
        } catch (Exception exc) {
            exc.printStackTrace();
        }
    }

    static Map<Integer, Guard> parseGuards(List<String> input) {
        List<LogEntry> logEntries = input.stream()
                .map(LogEntry::new)
                .sorted(Comparator.comparing(LogEntry::getEntryDateTime))
                .collect(Collectors.toList());

        Map<Integer, Guard> guards = new HashMap<>();
        int currGuardId = -1;
        for (LogEntry logEntry : logEntries) {
            currGuardId = logEntry.getGuardId() == -1 ? currGuardId : logEntry.getGuardId();
            if (!guards.containsKey(currGuardId)) {
                guards.put(currGuardId, new Guard(currGuardId));
            }

            guards.get(currGuardId).addLogEntry(logEntry);
        }

        guards.values().forEach(Guard::parseLogEntries);
        return guards;
    }

    static int solvePart1(List<String> input) {
        Map<Integer, Guard> guards = parseGuards(input);

        Guard sleepiestGuard = Collections.max(guards.values(), Comparator.comparing(Guard::getTotalSleep));

        return sleepiestGuard.getGuardId() * sleepiestGuard.getSleepiestMinute();
    }

    static int solvePart2(List<String> input) {
        Map<Integer, Guard> guards = parseGuards(input);

        Guard frequentSleeper = Collections.max(guards.values(), Comparator.comparing(g -> Collections.max(g.getSleepFrequency())));

        return frequentSleeper.getGuardId() * frequentSleeper.getSleepiestMinute();
    }

    public static void main(String[] args) {
        new Day4();
    }
}