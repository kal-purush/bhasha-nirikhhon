package com.codecool.ccms.models.factory;

import com.codecool.ccms.dao.UserDao;
import com.codecool.ccms.models.*;

public class UserFactory implements AbstractFactory<User> {

    private final UserDao userDao;

    public UserFactory(UserDao userDao) {
        this.userDao = userDao;
    }

    @Override
    public User create(int id, String firstName, String surname, String email, String password, Role role) {

        switch (role.toString()) {
            case "MANAGER":
                User manager = new Manager(id, firstName, surname, email, password, role);
                return manager;

            case "MENTOR":
                User mentor = new Mentor(id, firstName, surname, email, password, role);
                return mentor;

            case "ADMINEMPLOYEE":
                User adminEmployee = new AdminEmployee(id, firstName, surname, email, password, role);
                return adminEmployee;

            case "STUDENT":
                User student = new Student(id, firstName, surname, email, password, role);
                return student;

            default:
                return null;

        }
    }
}