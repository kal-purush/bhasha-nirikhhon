package tableUpdates;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import java.util.Stack;

public class UpdateFileReader {

    public UpdateFileReader() {
    }

    public static Stack<Operation> getUpdatesAsStack() {
        Stack<Operation> stack = new Stack<>();
        Scanner input = null;
        File file = new File("updates.txt");
        try {
            input = new Scanner(file);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        while(input.hasNextLine()) {
            Operation op = Operation.readFromFile(input.nextLine());
            stack.push(op);
        }

        return stack;
    }
}