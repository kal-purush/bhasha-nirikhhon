import java.util.Scanner;

class AliceOrBob {
    public static void main(String[] args) {
        System.out.print("What is your name? ");
        String UserName = "";
        Scanner UserNameImput = new Scanner(System.in);
        UserName = UserNameImput.nextLine();
        //Usernsme is Alice
        if (UserName.equalsIgnoreCase("Alice")) {
            System.out.println("Nice to meet you" + UserName);
        //Username is Bob
        } else if (UserName.equalsIgnoreCase("Bob")) {
            System.out.println("Nice to meet you!" + UserName);
        } else {
            System.out.println("Goodbye");
        }
    }
}