package ru.epam.javacore.lesson_23_relational_db.lesson;

import java.sql.Connection;
import java.sql.DriverManager;

public class A_2_H2_SimpleConnection implements A_1_Connectible {

    private static final A_2_H2_SimpleConnection INSTANCE = new A_2_H2_SimpleConnection();

    private A_2_H2_SimpleConnection() {

    }

    public static A_2_H2_SimpleConnection getInstance() {
        return INSTANCE;
    }

    @Override
    public Connection getConnection() throws Exception {
        String driverClass = "org.h2.Driver";
        Class.forName(driverClass);

        return DriverManager
                .getConnection("jdbc:h2:~/test33",
                               "sa",
                               "");
    }

}