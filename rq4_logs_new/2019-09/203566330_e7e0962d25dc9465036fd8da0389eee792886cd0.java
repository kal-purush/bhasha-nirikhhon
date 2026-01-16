import java.io.*;
import java.net.URL;
import java.util.ArrayList;

public class Tasklist {
    ArrayList<Task> list = new ArrayList<>();
    public Tasklist(){}
    public Tasklist(String input) throws DukeException {
        String[] splitTasks = input.split("\n");
        try {
            for (int i = 0; i < splitTasks.length; i++) {
                String split[] = splitTasks[i].split("\\s*\\|\\s*");
                if (split[0].equals("T"))
                    list.add(new Todo(split[1], split[2]));
                else if (split[0].equals("D"))
                    list.add(new Deadline(split[1], split[2], split[3]));
                else if (split[0].equals("E"))
                    list.add(new Event(split[1], split[2], split[3]));
                else
                    throw new DukeException((i+1) + "has incorect task format.");
            }
        }
        catch(DukeException e)
        {
            list.clear();
            throw new DukeException("Issues encountered when creating tasks, initializing empty list.");
        }
    };
    public long size(){
        return list.size();
    }
    public boolean isOutOfRange(int request){
        return ((request < 0) || (request >= this.size()));
    }

    public void markDone(int request){
        this.list.get(request).markDone();
        System.out.println("Nice! I've marked this task as done:\n" +
                "  " + this.list.get(request).toList());
    }
    public void banishDelete(int request){
        System.out.println("Noted. I've removed this task:\n" +
                "  " + list.get(request).toList());
        this.list.remove(request);
        System.out.println("Now you have " + this.list.size() + " tasks in the list.");
    }
    public void add(String type, String input){
        Task temp = null;
        try {
            switch (type) {
                case "todo":
                    temp = new Todo(input);
                    break;
                case "deadline":
                    temp = new Deadline(input);
                    break;
                case "event":
                    temp = new Event(input);
                    break;
                default:
                    throw new DukeException("What the Hell happened here?\n"+
                            "Command passed successfully to tasklist.add, not found in any case");
            };
        }
        catch (DukeException e) {
            System.out.println(e.getLocalizedMessage());
        }
        finally{
            if(temp != null) {
                this.list.add(temp);
                System.out.println("Got it. I've added this task:\n  " +
                        temp.toList() + "\nNow you have "+ this.size() + " tasks in the list.");
            }
        }
    }
    public void print() {
        int counter = 1;
        for (Task task : list) {
            System.out.println(counter++ + ". " + task.toList());
        }
    }
}