import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class SampleAnimal extends Animal {
    
    // Characteristics shared by all foxes (class variables).
    
    // Zombies do not breed, they infect. Thus, breeding age is set to 0.
    private static final int BREEDING_AGE = 0;
    // The age to which a Zombie can live, they do not die, but eventually decompose.
    private static final int MAX_AGE = 2147483647;
    // Zombies will always successfully infect prey.
    private static final double BREEDING_PROBABILITY = 1;
    // Zombies can infect one animal at a time.
    private static final int MAX_LITTER_SIZE = 1;

    private int willMove = 3;
    
    // A shared random number generator to control breeding.
    private static final Random rand = Randomizer.getRandom();
    
    public SampleAnimal(Field field, Location location){
        super(field, location);
    }

    @Override
    public void act(List<Animal> newAnimals) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'act'");
    }

    @Override
    protected int getBreedingAge() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getBreedingAge'");
    }

    @Override
    protected int getMaxAge() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getMaxAge'");
    }

    @Override
    protected double getBreedingProbability() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getBreedingProbability'");
    }

    @Override
    protected int getMaxLitterSize() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getMaxLitterSize'");
    }

    
    
    

}