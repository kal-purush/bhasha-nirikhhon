import java.util.Scanner;

public class ConsoleInput implements Observer {
    private ConsoleOutput output;
    private static SensorEvent handler;

    public ConsoleInput(ConsoleOutput output)   {
        this.output = output;

        this.output.addObserver(this);
    }

    @Override
    public void update() {
        int newState = output.getState();
    }

    public void setHandler(SensorEvent handler) {
        this.handler = handler;
    }

    public static void checkForTraffic()   {
        try(Scanner scan = new Scanner(System.in)) {
            String message;
            do {
                System.out.println("You are at a crossing.\nIs a car coming?\nyes / no");
                message = scan.next();
                if (message.equals("yes")) {
                    InputEvent e = new InputEvent("yes");
                    handler.notify(e);
                } else if (message.equals("no")) {
                    InputEvent e = new InputEvent("no");
                    handler.notify(e);
                } else {
                    System.out.println("invalid entry");
                }
            } while(message != "exit");
        }
    }

}