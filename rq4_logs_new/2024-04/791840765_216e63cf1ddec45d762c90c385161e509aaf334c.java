package pro.sky.List.Homework;
import java.util.Objects;

public class Employee {
    private String lastName;
    private String firstName;

    public Employee(String lastName, String firstName) {
        this.lastName = lastName;
        this.firstName = firstName;
    }

    String getLastName() { // Геттер
        return lastName;
    }

    String getFirstName() {
        return firstName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Employee employee = (Employee) o;
        return Objects.equals(lastName, employee.lastName) && Objects.equals(firstName, employee.firstName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lastName, firstName);
    }

    @Override
    public String toString() {
        return "{" +
                "\"firstName\" : \"" + firstName + "\" , \"" +
                "lastName\" : \"" + lastName + "\"" +
                "}";
    }
}