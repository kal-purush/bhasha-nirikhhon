package am.te.myapplication;
import java.util.ArrayList;


/**
 * Created by Mike Adkison on 2/9/15.
 */
public class RegistrationModel {

    private static ArrayList<User> users = new ArrayList<>();

    public static ArrayList<User> getUsers() {
        return users;
    }
    public static boolean addUser(User user) {
       for(User currUser: getUsers()) {
           if (currUser.getUsername().equals(user.getUsername())) {
               return false;
           }
       }
       users.add(user);
       return true;

    }
    public static void removeUser(User user) {
        users.remove(user);
    }
}