import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Scanner;
import java.util.List;

/**
 * 3
 * Created by Susanna.Aghlamazyan on 8/29/2016.
 */
public class Controller {
    String FILE_DIR = System.getProperty("user.dir") + "\\Homework9Modified\\resources\\config.properties";
    static int count = 1;
    String tempUserName;
    String tempPassword;
    List<User> userList = new ArrayList<User>();
    UserRepository newUserRep = new UserRepository();
    static User newU;

    Properties property = new Properties();
    InputStream input = null;


    Scanner sc = new Scanner(System.in);
    String insertedValue;

    public void startValidation() {
        try {
            input = new FileInputStream(FILE_DIR);
            property.load(input);
            System.out.print(property.getProperty("writeSignInOrSignUp"));
            insertedValue = sc.nextLine();

            while (1 == 1) {
                switch (insertedValue) {
                    case ("Sign Up"):
                        signUp();
                        break;
                    case ("Sign In"):
                        signIn();
                        break;
                    case ("Exit"):
                        System.exit(0);
                        break;
                    case ("Help"):
                        break;
                    case ("Add Number"):
                        addNumber(newU);
                        break;
                    case("Show Number"):
                        showNumber(newU);
                        break;
                    default:
                        System.out.println(property.getProperty("wrongCommand"));
                        System.out.println(property.getProperty("writeSignInOrSignUp"));
                        break;
                }
                insertedValue = sc.nextLine();
            }


        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

    }

    public void signUp() {
        System.out.print(property.getProperty("provideUsername"));
        String tempUserName = sc.nextLine();
        System.out.print(property.getProperty("providePassword"));
        String tempPassword = sc.nextLine();
        User newUser = new User(tempUserName, tempPassword);
        userList.add(newUser);
        newUserRep.addUser(newUser);
        System.out.print(property.getProperty("userCreatedSuccessfully"));
    }

    public void signIn() {

        boolean result = false;
        System.out.print(property.getProperty("provideUsername"));
        tempUserName = sc.nextLine();
        System.out.print(property.getProperty("providePassword"));
        tempPassword = sc.nextLine();
        User tempUser = new User(tempUserName, tempPassword);
        for (User u : userList) {
            if (u.equals(tempUser)) {
                result = true;
                newU=u;
                break;
            }
        }
        if (result) {

            System.out.print(property.getProperty("userLoggedSuccessfully"));
            System.out.print(property.getProperty("commandNumberOrShowNumber"));

        } else if (count < 3) {
            System.out.println(property.getProperty("wrongUserPass"));
            count++;
            signIn();
        } else {
            System.out.println(property.getProperty("lockAccount"));
            System.exit(0);
        }

    }

    public void addNumber(User user){
        System.out.print(property.getProperty("enterPhoneNumber"));
        insertedValue =sc.nextLine();
        user.phoneNumber.add(new Number(insertedValue));
        System.out.print(property.getProperty("commandAfterInputShowOrAdd"));
    }

    public void showNumber(User user){
        for(Number num: user.phoneNumber){
            System.out.println(num.getPhoneNumber());
        }
    }

}