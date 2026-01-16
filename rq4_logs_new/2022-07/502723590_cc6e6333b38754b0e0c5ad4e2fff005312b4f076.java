package SQL;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class SQL_create_base {
    public static void main(String[] args) {
        try {
            String url = "jdbc:mysql://localhost/ms_db?serverTimezone=Europe/Kiev&useSSL=false&useUnicode=true&characterEncoding=utf-8";
            String username = "root";
            String password = "root";
            Connection conn = DriverManager.getConnection(url, username, password);
            Statement stat = conn.createStatement();
            stat.executeUpdate("DROP TABLE IF EXISTS students;");
            stat.executeUpdate("CREATE TABLE students "
                    + "(student_id INT NOT NULL AUTO_INCREMENT, student_name VARCHAR(255), student_surname VARCHAR(255), "
                    + "student_patronymic VARCHAR(255), student_date DATE, student_adress VARCHAR(255), PRIMARY KEY (student_id));");
            stat.executeUpdate("DROP TABLE IF EXISTS education_part;");
            stat.executeUpdate("CREATE TABLE education_part (id INT NOT NULL AUTO_INCREMENT, student_id INT, group_name VARCHAR(255), GPA DOUBLE, visiting INT, PRIMARY KEY (id));");
            stat.executeUpdate("DROP TABLE IF EXISTS themes;");
            stat.executeUpdate("CREATE TABLE themes (name_theme VARCHAR(255), num_hours INT);");
            stat.executeUpdate("DROP TABLE IF EXISTS professors;");
            stat.executeUpdate("CREATE TABLE professors (id INT NOT NULL AUTO_INCREMENT, prof_name VARCHAR(255), prof_surname VARCHAR(255), prof_patronymic VARCHAR(255), post VARCHAR(255), PRIMARY KEY (id));");

            stat.executeUpdate("INSERT INTO students (student_name, student_surname, student_patronymic, student_date, student_adress) VALUES ('Иван', 'Иванов', 'Иванович', '1998.09.13', 'Киев');");
            stat.executeUpdate("INSERT INTO students (student_name, student_surname, student_patronymic, student_date, student_adress) VALUES ('Андрей', 'Петренко', 'Евгениевич', '1997.10.13', 'Киев');");
            stat.executeUpdate("INSERT INTO students (student_name, student_surname, student_patronymic, student_date, student_adress) VALUES ('Ярослав', 'Коваленко', 'Виталиевич', '1997.06.15', 'Днепр');");
            stat.executeUpdate("INSERT INTO students (student_name, student_surname, student_patronymic, student_date, student_adress) VALUES ('Лидия', 'Ярошенко', 'Владимировна', '1998.12.13', 'Херсон');");
            stat.executeUpdate("INSERT INTO students (student_name, student_surname, student_patronymic, student_date, student_adress) VALUES ('Карина', 'Шевченко', 'Александровна', '1999.08.01', 'Винница');");
            stat.executeUpdate("INSERT INTO students (student_name, student_surname, student_patronymic, student_date, student_adress) VALUES ('Лиза', 'Колесниченко', 'Сергеевна', '1998.03.17', 'Кропивницкий');");
            stat.executeUpdate("INSERT INTO students (student_name, student_surname, student_patronymic, student_date, student_adress) VALUES ('Наталья', 'Порошенко', 'Андреевна', '1997.04.26', 'Чернигов');");
            stat.executeUpdate("INSERT INTO students (student_name, student_surname, student_patronymic, student_date, student_adress) VALUES ('Анастасия', 'Волосковец', 'Олеговна', '2000.05.13', 'Одесса');");
            stat.executeUpdate("INSERT INTO students (student_name, student_surname, student_patronymic, student_date, student_adress) VALUES ('Алина', 'Маляцкая', 'Ярославовна', '1997.02.28', 'Житомир');");
            stat.executeUpdate("INSERT INTO students (student_name, student_surname, student_patronymic, student_date, student_adress) VALUES ('Виталий', 'Тригуб', 'Викторович', '1999.10.03', 'Львов');");

            stat.executeUpdate("INSERT INTO themes (name_theme, num_hours) VALUES ('математика', 42);");
            stat.executeUpdate("INSERT INTO themes (name_theme, num_hours) VALUES ('география', 18);");
            stat.executeUpdate("INSERT INTO themes (name_theme, num_hours) VALUES ('биология', 24);");
            stat.executeUpdate("INSERT INTO themes (name_theme, num_hours) VALUES ('информатика', 36);");

            stat.executeUpdate("INSERT INTO professors (prof_name, prof_surname, prof_patronymic, post) VALUES ('Ирина', 'Юрина', 'Викторовна', 'учитель математики')");
            stat.executeUpdate("INSERT INTO professors (prof_name, prof_surname, prof_patronymic, post) VALUES ('Ольга', 'Галаган', 'Олександровна', 'учитель географии')");
            stat.executeUpdate("INSERT INTO professors (prof_name, prof_surname, prof_patronymic, post) VALUES ('Светлана', 'Габдрахманова', 'Єргюновна', 'учитель биологии и информатики')");

            stat.executeUpdate("INSERT INTO education_part (student_id, group_name, GPA, visiting) VALUES (1, 'ФМ', 3.9, 28);");
            stat.executeUpdate("INSERT INTO education_part (student_id, group_name, GPA, visiting) VALUES (2, 'ФМ', 4.9, 38);");
            stat.executeUpdate("INSERT INTO education_part (student_id, group_name, GPA, visiting) VALUES (3, 'ФМ', 3.1, 26);");
            stat.executeUpdate("INSERT INTO education_part (student_id, group_name, GPA, visiting) VALUES (4, 'ГМ', 3.2, 48);");
            stat.executeUpdate("INSERT INTO education_part (student_id, group_name, GPA, visiting) VALUES (5, 'ГМ', 4.5, 35);");
            stat.executeUpdate("INSERT INTO education_part (student_id, group_name, GPA, visiting) VALUES (6, 'ГМ', 4.0, 29);");
            stat.executeUpdate("INSERT INTO education_part (student_id, group_name, GPA, visiting) VALUES (7, 'ТХ', 4.1, 39);");
            stat.executeUpdate("INSERT INTO education_part (student_id, group_name, GPA, visiting) VALUES (8, 'ТХ', 4.6, 38);");
            stat.executeUpdate("INSERT INTO education_part (student_id, group_name, GPA, visiting) VALUES (9, 'ИМ', 3.8, 44);");
            stat.executeUpdate("INSERT INTO education_part (student_id, group_name, GPA, visiting) VALUES (10, 'ИМ', 3.6, 44);");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}