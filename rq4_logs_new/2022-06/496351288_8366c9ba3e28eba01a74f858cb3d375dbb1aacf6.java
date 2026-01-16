package MainThrow;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

public class Main {
    public static void main(String[] args) {
        Employee employee = new Employee();
        try {
            employee.setAge(-10);

        } catch (InvalidAgeException e) {

            e.printStackTrace();
        }
    }


}

class Employee {
    private int age;

    public void setAge(int age) throws InvalidAgeException {
        if (age >= 0 & age < 200) {
            this.age = age;
        } else {
            throw new InvalidAgeException(age);
        }

    }
}