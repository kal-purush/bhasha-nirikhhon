import java.util.*;
public class index {
    Scanner input = new Scanner(System.in);

    int id=0;

    public void signin(){
        System.out.println("Enter Username");
            String uname = input.nextLine();
        System.out.println("Enter Password");
            String pword = input.nextLine();

        if(verify(uname,pword))
            loggedin();
        else {
            System.out.println("Invalid Username or Password");
            signin();
        }
    }

    public void signup(){
        System.out.println("Enter Your Name");
        String name = input.nextLine();

        System.out.println("Enter Custom UserName");
        String uname = input.nextLine();

            if(check(uname)) {

                System.out.println("Enter Password");
                String p1word = input.nextLine();
                System.out.println("Re-enter Password");
                String p2word = input.nextLine();

                if(p1word.equals(p2word)) {
                    System.out.println("Your Id is " + ++id);
                    System.out.println("Your UserName is "+uname);

                    System.out.println("Do you want to sign in?");
                    char ch = input.next().charAt(0);
                    if(ch=='y') {
                        signin();
                    }
                    else
                }
            }
    }

    public static void main(String[] args) {


        System.out.println("Sign in \n Sign up ");
        int choice = input.nextInt();
        switch (choice)
            {
                case 1: signin();
                    break;
                case 2: signup();
            }

        }

}
//sign in
// sign up