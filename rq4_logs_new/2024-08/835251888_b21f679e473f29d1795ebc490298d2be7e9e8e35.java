package dev.forkingaround.zootopia.models;

import org.junit.jupiter.api.Test;
import java.util.Date;
import static org.junit.jupiter.api.Assertions.*;

class AnimalTest {

    @Test
    void testAnimalConstructorAndGetters() {
        Date dateOfEntry = new Date();
        Type type = new Type();
        type.setId(1L);
        type.setName("Mammal");

        Animal animal = new Animal(1L, "Lion", "Male", dateOfEntry, type);

        assertEquals(1L, animal.getId());
        assertEquals("Lion", animal.getName());
        assertEquals("Male", animal.getGender());
        assertEquals(dateOfEntry, animal.getDateOfEntry());
        assertEquals(type, animal.getType());
    }

    @Test
    void testAnimalBuilder() {
        Date dateOfEntry = new Date();
        Type type = new Type();
        type.setId(2L);
        type.setName("Bird");

        Animal animal = Animal.builder()
                .id(2L)
                .name("Eagle")
                .gender("Female")
                .dateOfEntry(dateOfEntry)
                .type(type)
                .build();

        assertEquals(2L, animal.getId());
        assertEquals("Eagle", animal.getName());
        assertEquals("Female", animal.getGender());
        assertEquals(dateOfEntry, animal.getDateOfEntry());
        assertEquals(type, animal.getType());
    }

    @Test
    void testAnimalSetters() {
        Animal animal = new Animal();
        Date dateOfEntry = new Date();
        Type type = new Type();
        type.setId(3L);
        type.setName("Reptile");

        animal.setId(3L);
        animal.setName("Crocodile");
        animal.setGender("Male");
        animal.setDateOfEntry(dateOfEntry);
        animal.setType(type);

        assertEquals(3L, animal.getId());
        assertEquals("Crocodile", animal.getName());
        assertEquals("Male", animal.getGender());
        assertEquals(dateOfEntry, animal.getDateOfEntry());
        assertEquals(type, animal.getType());
    }

    @Test
    void testEqualsAndHashCode() {
        Date dateOfEntry = new Date();
        Type type = new Type();
        type.setId(4L);
        type.setName("Fish");

        Animal animal1 = new Animal(4L, "Shark", "Female", dateOfEntry, type);
        Animal animal2 = new Animal(4L, "Shark", "Female", dateOfEntry, type);
        Animal animal3 = new Animal(5L, "Dolphin", "Male", dateOfEntry, type);

        assertEquals(animal1, animal2);
        assertNotEquals(animal1, animal3);
        assertEquals(animal1.hashCode(), animal2.hashCode());
        assertNotEquals(animal1.hashCode(), animal3.hashCode());
    }

    @Test
    void testToString() {
        Date dateOfEntry = new Date();
        Type type = new Type();
        type.setId(5L);
        type.setName("Amphibian");

        Animal animal = new Animal(5L, "Frog", "Male", dateOfEntry, type);
        String toStringResult = animal.toString();

        assertTrue(toStringResult.contains("id=5"));
        assertTrue(toStringResult.contains("name=Frog"));
        assertTrue(toStringResult.contains("gender=Male"));
        assertTrue(toStringResult.contains("dateOfEntry=" + dateOfEntry));
        assertTrue(toStringResult.contains("type=" + type));
    }
}