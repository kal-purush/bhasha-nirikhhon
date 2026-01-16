package by.teachmeskills.interfacesandinheritence.employee;

import java.util.Objects;

public class Accountant implements Employee {
    private final String name;
    private final String surname;

    public Accountant(String name, String surname) {
        this.name = name;
        this.surname = surname;
    }

    @Override
    public void printJobTitle() {
        System.out.println("Job title of " + name + " " + surname + " is: "
                + EmployeeJobTitle.ACCOUNTANT.getJobTitle());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Accountant)) return false;
        Accountant that = (Accountant) o;
        return name.equals(that.name) && surname.equals(that.surname);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, surname);
    }

    @Override
    public String toString() {
        return "Accountant{" +
                "name='" + name + '\'' +
                ", surname='" + surname + '\'' +
                '}';
    }
}