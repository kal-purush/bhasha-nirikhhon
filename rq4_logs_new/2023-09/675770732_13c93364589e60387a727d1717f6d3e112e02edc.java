package datastructures.pseudoQueue;

import dataStructures.animalShelter.Animal;
import dataStructures.animalShelter.AnimalShelter;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestAnimalShelter {

    @Test
    public void testEnqueueAndDequeue() {
        AnimalShelter shelter = new AnimalShelter();
        shelter.enqueue(new Animal("dog", "Buddy"));
        shelter.enqueue(new Animal("cat", "Whiskers"));
        shelter.enqueue(new Animal("dog", "Max"));

        assertEquals("Buddy", shelter.dequeue("dog").getName());
        assertEquals("Whiskers", shelter.dequeue("cat").getName());
        assertEquals("Max", shelter.dequeue("dog").getName());
        assertNull(shelter.dequeue("cat"));
    }

    @Test
    public void testDequeueWithEmptyShelter() {
        AnimalShelter shelter = new AnimalShelter();
        assertNull(shelter.dequeue("dog"));
        assertNull(shelter.dequeue("cat"));
    }

    @Test
    public void testDequeueWithWrongPreference() {
        AnimalShelter shelter = new AnimalShelter();
        shelter.enqueue(new Animal("dog", "Buddy"));
        shelter.enqueue(new Animal("cat", "Whiskers"));

        assertNull(shelter.dequeue("rabbit"));
    }
}