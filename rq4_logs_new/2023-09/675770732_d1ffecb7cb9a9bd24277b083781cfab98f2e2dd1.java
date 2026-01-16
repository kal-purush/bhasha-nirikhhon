package dataStructures.animalShelter;

import java.util.LinkedList;
import java.util.Queue;

public class AnimalShelter {
    private Queue<Animal> dogQueue;
    private Queue<Animal> catQueue;

    public AnimalShelter() {
        dogQueue = new LinkedList<>();
        catQueue = new LinkedList<>();
    }

    public void enqueue(Animal animal) {
        if (animal.getSpecies().equals("dog")) {
            dogQueue.offer(animal);
        } else if (animal.getSpecies().equals("cat")) {
            catQueue.offer(animal);
        }
    }
    public Animal dequeue(String pref) {
        if (pref.equals("dog") && !dogQueue.isEmpty()) {
            return dogQueue.poll();
        } else if (pref.equals("cat") && !catQueue.isEmpty()) {
            return catQueue.poll();
        } else {
            return null; // Return null if the preferred species is not available
        }
    }
}