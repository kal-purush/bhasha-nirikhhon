package tec.poo;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import tec.poo.picocli.cli.CreateEmployeeCommand;
import tec.poo.picocli.cli.MainCommand;

//@CommandLine.Command and(name = "app", mixinStandardHelpOptions = true, version = "1.0",
//        description = "PicoCLI application with interactive mode")
//public class App  {
//    private static final Logger logger = LoggerFactory.getLogger(App.class);
//
//    public static void main( String[] args ) {
//        CommandLine cmd = new CommandLine(new MainCommand());
//        cmd.setExecutionStrategy(new CommandLine.RunAll()); // default is RunLast
//        cmd.execute(args);
//
//        if (args.length == 0) {
//            cmd.usage(System.out);
//        }
//    }
//}

import java.util.Scanner;

@Command(name = "menu", mixinStandardHelpOptions = true, version = "1.0",
        description = "Interactive menu example using PicoCLI")
public class App implements Runnable {

    @Option(names = {"-v", "--verbose"}, description = "Verbose mode")
    private boolean verbose;

    public static void main(String... args) {
        // If command-line arguments are provided, run in regular CLI mode
        if (args.length > 0) {
            CommandLine cmd = new CommandLine(new MainCommand());
            cmd.execute(args);
        } else {
            // Otherwise, run in interactive mode
            App interactiveMenu = new App();
            interactiveMenu.run();
        }
    }

    @Override
    public void run() {

        // Main logic of your application goes here
        System.out.println("Interactive menu example");

        // Display the menu and handle user input
        displayMenu();
    }

    private void displayMenu() {
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.println("\nMenu:");
            System.out.println("1. Reports");
            System.out.println("2. New Item");
            System.out.println("3. Update Item");
            System.out.println("4. Remove Item");
            System.out.println("5. Exit");

            System.out.print("Select an option: ");
            String input = scanner.nextLine();

            switch (input) {
                case "1":
                    System.out.println("Generating reports...");
                    break;
                case "2":
                    System.out.println("Creating a new item...");
                    new CommandLine(new CreateEmployeeCommand()).execute();
                    break;
                case "3":
                    System.out.println("Updating an existing item...");
                    break;
                case "4":
                    System.out.println("Removing an existing item...");
                    break;
                case "5":
                    return;
                default:
                    System.out.println("Invalid option. Please try again.");
            }
        }
    }
}