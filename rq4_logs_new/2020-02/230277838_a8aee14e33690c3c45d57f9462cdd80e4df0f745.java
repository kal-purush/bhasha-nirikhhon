package ru.epam.javacore.lesson_24_db_web.lesson.db;

import ru.epam.javacore.lesson_12_io_nio.homework.carrier.domain.Carrier;
import ru.epam.javacore.lesson_4_oop_continue.homework.cargo.Cargo;

import java.util.List;

import static ru.epam.javacore.lesson_24_db_web.lesson.db.DbUtils.executeUpdate;

public class AnimalRepo {

    public boolean save(Animal animal) {
        int affectedRows = executeUpdate(
                "INSERT INTO ANIMAL (ID, NAME) VALUES (?,?)",
                ps -> {
                    int i = 0;
                    ps.setLong(++i, animal.getId());
                    ps.setString(++i, animal.getName());
                }
        );

        return affectedRows == 1;
    }

    public boolean update(Animal animal) {
        int affectedRows = executeUpdate(
                "UPDATE ANIMAL SET NAME=? WHERE ID =?",
                ps -> {
                    int i = 0;
                    ps.setString(++i, animal.getName());
                    ps.setLong(++i, animal.getId());
                }
        );

        return affectedRows == 1;
    }

    public List<Animal> getAllAnimals() {
        return DbUtils.selectAll("SELECT * FROM ANIMAL",
                                 AnimalMapper::mapAnimal);
    }

}