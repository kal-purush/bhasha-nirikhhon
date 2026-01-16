import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

public class Statistics {
    private List<Task> tasksDone;

    public Statistics() {
        tasksDone = new LinkedList<>();
        Path file = Paths.get("stats.txt");
        try {
            Scanner fileSc = new Scanner(file).useDelimiter("\\||\\n");
            while (fileSc.hasNext()) {
                String line = fileSc.nextLine();
                String[] lineBreakUp = line.split(" \\| ");
                switch (lineBreakUp[0]) {
                    case "T":
                        tasksDone.add(new toDo(lineBreakUp[2], lineBreakUp[1].equals("1")));
                        break;
                    case "D":
                        tasksDone.add(new Deadline(lineBreakUp[2], lineBreakUp[3], lineBreakUp[1].equals("1")));
                        break;
                    case "E":
                        tasksDone.add(new Event(lineBreakUp[2], lineBreakUp[3], lineBreakUp[1].equals("1")));
                        break;
                    default:
                        System.out.println("wrong input from file");
                }
            }
        } catch (IOException exp) {
            System.out.println("ioException caught when loading file!");
        }
    }
    public void addToDone(Task task) {
        tasksDone.add(task);
    }
    public String listDone(){
        StringBuilder str = new StringBuilder();
        str.append("Here are the tasks you have done:\n");
        for (Task t : tasksDone) {
            str.append(t.toString());
        }
        return str.toString();
    }

    public void save() {
        List<String> lines = new LinkedList<>();
        for (Task t : tasksDone) {
            lines.add(t.toSave());
        }
        Path file = Paths.get("stats.txt");
        try {
            Files.write(file, lines, StandardCharsets.UTF_8);
        } catch (IOException ex) {
            System.out.println("error when saving stats");
        }
    }
}