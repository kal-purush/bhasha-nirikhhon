package general;

import lombok.Data;

import static utils.RandomGenerator.generateRandomEmail;
import static utils.RandomGenerator.generateRandomMobileNumber;
import static utils.RandomGenerator.generateRandomString;

@Data
public class User {

    private String firstName;
    private String lastName;
    private String mobileNumber;
    private String emailAddress;
    private String password;

    public User() {
        this.firstName = generateRandomString();
        this.lastName = generateRandomString();
        this.mobileNumber = generateRandomMobileNumber();
        this.emailAddress = generateRandomEmail();
        this.password = generateRandomString();
    }

    public User(String firstName, String lastName) {
        this.firstName = firstName;
    }
}