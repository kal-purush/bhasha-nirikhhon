import java.util.*;
import java.awt.Color;
/**
 * Write a description of class PopulationGenerator here.
 *
 * @author (your name)
 * @version (a version number or a date)
 */
public class PopulationGenerator
{
    
    // Constants representing configuration information for the simulation.
    // The default width for the grid.
    private static final int DEFAULT_WIDTH = 120;
    // The default depth of the grid.
    private static final int DEFAULT_DEPTH = 80;
    // The probability that a fox will be created in any given grid position.
    private static final double FOX_CREATION_PROBABILITY = 0.02;
    // The probability that a rabbit will be created in any given position.
    private static final double RABBIT_CREATION_PROBABILITY = 0.08;    

    // Lists of animals in the field.
    private List<Animal> animals;
    
    // The current state of the field.
    private Field field;
    private SimulatorView view;
    public PopulationGenerator(Field field, List<Animal> animals)
    {
        this.field = field;
        this.animals = animals;
        
    }
    
    public void setViewColors(SimulatorView view)
    {
       
       view.setColor(Rabbit.class, Color.ORANGE);
       view.setColor(Fox.class, Color.BLUE); 
    }
    
    public void populate()
    {
        Random rand = Randomizer.getRandom();
        
        for(int row = 0; row < field.getDepth(); row++) {
            for(int col = 0; col < field.getWidth(); col++) {
                if(rand.nextDouble() <= FOX_CREATION_PROBABILITY) {
                    Location location = new Location(row, col);
                    Fox fox = new Fox(true, field, location);
                    animals.add(fox);
                }
                else if(rand.nextDouble() <= RABBIT_CREATION_PROBABILITY) {
                    Location location = new Location(row, col);
                    Rabbit rabbit = new Rabbit(true, field, location);
                    animals.add(rabbit);
                }
                // else leave the location empty.
            }
        }
    }
    
    
}