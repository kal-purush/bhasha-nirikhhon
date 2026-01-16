package tec.poo.command;

public class DoWhileCommand {

    private String[] args;

    public DoWhileCommand(String[] args) {
        this.args = args;
    }

    public void execute() {
        int count = 0;

        // Example of using do-while loop to print numbers from 1 to 5
        System.out.println("Using do-while loop:");
        do {
            count++;
            System.out.println("Count: " + count);
        } while (count < 5);
    }

    @Override
    public String toString() {
        return "Do-While Example";
    }
}