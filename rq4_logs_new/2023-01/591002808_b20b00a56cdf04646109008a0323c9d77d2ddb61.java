public class Start {
    public static void main(String[] args) {

        String month = "January";

        switch (month) {
            case "January":
                System.out.println("Winter");
                break;
            case "October":
                System.out.println("Autumn");
                break;
            case "August":
                System.out.println("Summer :)");
                break;
            default:
                System.out.println("Spring :)");
        }

        System.out.println("===");

        month = "January";

        switch (month) {
            case "January":
                System.out.println("Winter");

            case "October":
                System.out.println("Autumn");

            case "August":
                System.out.println("Summer :)");
                break;
            default:
                System.out.println("Spring :)");
        }

        System.out.println("===");

        month = "January";

        switch (month) {
            case "January" -> System.out.println("Winter");

            case "October" -> System.out.println("Autumn");

            case "August" -> System.out.println("Summer :)");

            default -> System.out.println("Spring :)");
        }

        System.out.println("===");

        month = "January";
        String selection = "";

        selection = switch (month) {
            case "January" -> "Winter";
            case "October" -> "Autumn";
            case "August" -> "Summer :)";
            default -> "Spring :)";
        };

        System.out.println(selection);

        month = "January";
        selection = "";

        selection = switch (month) {
            case "January" : yield "Winter";
            case "October" : yield "Autumn";
            case "August" : yield "Summer :)";
            default: yield "Spring :)";
        };

        System.out.println(selection);
    }
}