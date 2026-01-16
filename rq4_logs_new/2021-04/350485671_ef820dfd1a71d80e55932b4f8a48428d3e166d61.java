package challenges.animalShelter;

import challenges.stacksQueues.LinkedListQueue;
import challenges.stacksQueues.Queue;

import java.util.NoSuchElementException;

public class AnimalShelter {
    long count = 0;
    Queue<TypedQueueEntry<Cat>> cats = new LinkedListQueue<TypedQueueEntry<Cat>>();
    Queue<TypedQueueEntry<Dog>> dogs = new LinkedListQueue<TypedQueueEntry<Dog>>();

    public void enqueue(Cat cat)  {
        cats.enqueue(new TypedQueueEntry<Cat>(cat, count++));
    }

    public void enqueue(Dog dog) {
        dogs.enqueue(new TypedQueueEntry<Dog>(dog, count++));
    }

    public Animal dequeue(String animalType) throws NoSuchElementException {
        if (animalType == "dog") return dogs.dequeue().item;
        if (animalType == "cat") return cats.dequeue().item;
        else throw new NoSuchElementException(String.format("No animal of type %s at this shelter", animalType));
    }

    public Animal dequeue() throws NoSuchElementException {
        if (cats.isEmpty()) return dogs.dequeue().item;
        if (dogs.isEmpty()) return cats.dequeue().item;
        return cats.peek().entered < dogs.peek().entered
                ? cats.dequeue().item
                : dogs.dequeue().item;
    }
}