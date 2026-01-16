import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Parser {
    public Parser() {}

    public static Command parse(String fullCommand) {
        int i = fullCommand.indexOf(' ');
        String first = getFirstWord(fullCommand);
        switch (first) {
            case "delete":
                return new DeleteCommand(Integer.parseInt(fullCommand.substring(i + 1)));
            case "done":
                return new DoneCommand(Integer.parseInt(fullCommand.substring(i + 1)));
            case "list":
                return new ListCommand();
            case "bye":
                return new ByeCommand();
            case "todo":
                if (fullCommand.length() == 4) {
                    System.out.println("☹ OOPS!!! The description of a todo cannot be empty.");
                } else {
                    ToDos taskTodo = new ToDos(fullCommand.substring(i + 1));
                    taskTodo.setTaskType("T");
                    return new AddCommand(taskTodo);
                }
            case "deadline":
                int j = fullCommand.indexOf("/");
                int k = fullCommand.indexOf(" ");

                if (fullCommand.length() == 8) {
                    System.out.println("☹ OOPS!!! The description of a deadline cannot be empty.");
                } else {
                    Deadlines taskDeadline = new Deadlines(fullCommand.substring(k + 1, j - 1));
                    taskDeadline.setTime(fullCommand.substring(j + 4));
                    taskDeadline.setTaskType("D");
                    return new AddCommand(taskDeadline);
                }
            case "event":
                int p = fullCommand.indexOf("/");
                int q = fullCommand.indexOf(" ");

                if (fullCommand.length() == 5) {
                    System.out.println("☹ OOPS!!! The description of an event cannot be empty.");
                } else {
                    Events taskEvent = new Events(fullCommand.substring(q + 1, p - 1));
                    taskEvent.setTime(fullCommand.substring(p + 4));
                    taskEvent.setTaskType("E");
                    return new AddCommand(taskEvent);
                }
            default:
                System.out.println("☹ OOPS!!! I'm sorry, but I don't know what that means :-()");
                return null;
        }
    }

        public static String getFirstWord(String input) {
            if (input.indexOf(" ") > -1) {
                return input.substring(0, input.indexOf(" "));
            } else {
                return input;
            }
        }
    }