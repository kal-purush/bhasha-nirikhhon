package managers.model;


import model.Epic;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;


public class EpicTest {

    // проверьте, что задачи с заданным id и сгенерированным id не конфликтуют внутри менеджера.
    // Независимо от переданного id программа присваивает свой и задачи считаются разными
    @Test
    public void equals_returnTrue_passedIdsMatch() {
        Epic epic1 = new Epic("name", "description", 1);
        Epic epic2 = new Epic("name", "description", 1);
        assertTrue(epic1.equals(epic2));
    }



}