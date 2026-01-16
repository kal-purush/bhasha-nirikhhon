package simulator;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Pattern;

public class InstructionFactory {

    private static final Map<Pattern, Function<String, InstructionCommand>> COMMAND_MAPPER = new HashMap<>();

    private static final Pattern PLACE_PATTERN = Pattern.compile("^PLACE (\\d+),(\\d+),(WEST|NORTH|SOUTH|EAST)$");
    private static final Pattern MOVE_PATTERN = Pattern.compile("^MOVE$");
    private static final Pattern ROTATE_RIGHT_PATTERN = Pattern.compile("^RIGHT$");
    private static final Pattern ROTATE_LEFT_PATTERN = Pattern.compile("^LEFT$");
    private static final Pattern REPORT_PATTERN = Pattern.compile("^REPORT$");

    static {
        COMMAND_MAPPER.put(PLACE_PATTERN, getPlaceCommand());
        COMMAND_MAPPER.put(MOVE_PATTERN, getMoveCommand());
        COMMAND_MAPPER.put(ROTATE_RIGHT_PATTERN, getRotateLRightCommand());
        COMMAND_MAPPER.put(ROTATE_LEFT_PATTERN, getRotateLeftCommand());
        COMMAND_MAPPER.put(REPORT_PATTERN, getReportCommand());
    }

    static Optional<InstructionCommand> of(String rawInstruction) {
        return COMMAND_MAPPER.entrySet()
                .stream()
                .filter(entry -> entry.getKey().matcher(rawInstruction).find())
                .findFirst()
                .map(Map.Entry::getValue)
                .map(f -> f.apply(rawInstruction));
    }

    private static Function<String, InstructionCommand> getPlaceCommand() {
        return rawInstruction -> robot -> {
            String[] values = rawInstruction.split("[, ]");
            int x = Integer.parseInt(values[1]);
            int y = Integer.parseInt(values[2]);
            Direction direction = Direction.valueOf(values[3]);
            robot.place(new Point(x, y), direction);
            return robot;
        };
    }

    private static Function<String, InstructionCommand> getMoveCommand() {
        return rawInstruction -> robot -> {
            robot.move();
            return robot;
        };
    }

    private static Function<String, InstructionCommand> getReportCommand() {
        return rawInstruction -> robot -> {
            robot.report();
            return robot;
        };
    }

    private static Function<String, InstructionCommand> getRotateLRightCommand() {
        return rawInstruction -> robot -> {
            robot.rotateRight();
            return robot;
        };
    }

    private static Function<String, InstructionCommand> getRotateLeftCommand() {
        return rawInstruction -> robot -> {
            robot.rotateLeft();
            return robot;
        };
    }

}