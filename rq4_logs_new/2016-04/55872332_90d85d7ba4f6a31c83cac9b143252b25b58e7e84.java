package ohtu.data_access;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;
import ohtu.domain.User;
import org.springframework.stereotype.Component;

@Component
public class FileUserDao implements UserDao {

    private String filename;

    public FileUserDao(String filename) {
        this.filename = filename;
    }

    public FileUserDao() {
        this("users.txt");
    }

    public void getUsers(ArrayList<User> users, Scanner scanner) {
        while (scanner.hasNextLine()) {
            String[] namePass = scanner.nextLine().split(" ");
            users.add(new User(namePass[0], namePass[1]));
        }
        scanner.close();
    }

    @Override
    public List<User> listAll() {
        ArrayList<User> users = new ArrayList<User>();
        try {
            Scanner scanner = new Scanner(new File(filename));
            getUsers(users, scanner);
        } catch (FileNotFoundException ex) {
            Logger.getLogger(FileUserDao.class.getName()).log(Level.SEVERE, null, ex);
        }
        return users;
    }

    @Override
    public User findByName(String name) {
        List<User> users = (ArrayList) listAll();
        for (User user : users) {
            if (user.getUsername().equals(name)) {
                return user;
            }
        }
        return null;
    }

    @Override
    public void add(User user) {
        try {
            FileWriter writer = new FileWriter(filename, true);

            writer.write(user.getUsername() + " " + user.getPassword() + "\n");
            writer.close();
        } catch (IOException ex) {
            Logger.getLogger(FileUserDao.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}