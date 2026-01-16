package tests;

import com.github.javafaker.Faker;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Locale;

public class TestDemoqaHW4WithPageObjectsJavaFaker extends TestBase {

    @Test
    void testDemoqa() {

        Faker faker = new Faker(new Locale("en"));
        File file = new File("src/test/resources/readme.txt");

        String firstName = faker.name().firstName(),
                lastName = faker.name().lastName(),
                userEmail = faker.internet().emailAddress(),
                address = faker.address().streetAddress(),
                userNumber = faker.number().digits(10),
                hobbies = faker.options().option("Reading", "Sports", "Music"),
                gender = faker.options().option("Female", "Male", "Other"),
                subjects = faker.options().option("Biology", "Englis", "Chemistry", "Computer Science",
                        "Commerce", "Economics", "Social Studies", "Arts", "Histpory", "Maths", "Accounting", "Physics",
                        "Hindi", "Civics"),
                selectState = faker.options().option("Haryana"),
                selectCity = faker.options().option("Karnal", "Panipat");


        registrationPage.openPage()
                .setFirstName(firstName)
                .setLastName(lastName)
                .setEmail(userEmail)
                .setGender(gender)
                .setPhone(userNumber)
                .setBirthDate("13", "March", "1992")
                .setSubjects(subjects)
                .setHobbies(hobbies)
                .uploadPicture(file)
                .setCurrentAddress(address)
                .selectState(selectState)
                .selectCity(selectCity)
                .clickButton();

        registrationPage.verifyResultsModalAppears()
                .verifyResult("Student Name", firstName + " " + lastName)
                .verifyResult("Student Email", userEmail)
                .verifyResult("Gender", gender)
                .verifyResult("Mobile", userNumber)
                .verifyResult("Date of Birth", "13 March,1992")
                .verifyResult("Subjects", subjects)
                .verifyResult("Hobbies", hobbies)
                .verifyResult("Picture", "readme.txt")
                .verifyResult("Address", address)
                .verifyResult("State and City", selectState + " " + selectCity);

    }
}