import java.util.Scanner;

public class Parser {
    public Parser(){};
    public static void parse(String line) throws DukeException {
        Scanner temp = new Scanner(line);
        String command = temp.next();
        if (command.matches("list|bye")) {
            if (temp.hasNextLine()) {
                throw new DukeException("List should not have any other arguments (whitespace acceptable");
            }
            else{
                //for the future
            }
        }
        else if (command.matches("todo|deadline|event|done|delete")) {
            if(!temp.hasNextLine())
                throw new DukeException("☹ OOPS!!! The description of a " + command + " cannot be empty.");
            else {
                String input = temp.nextLine();
                try{
                    if(command.matches("done|delete")) {
                        int request = Integer.parseInt(input);
                    }
                } catch (NumberFormatException e) {
                    System.out.println("That is NOT a valid integer");
                }
            }
        }
        else
            throw new DukeException("☹ OOPS!!! I'm sorry, but I don't know what that means :-(");
    }
}