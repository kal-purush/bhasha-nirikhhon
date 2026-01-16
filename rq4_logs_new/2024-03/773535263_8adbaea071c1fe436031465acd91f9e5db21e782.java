import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class Eagle extends Animal{

    private static final int BREEDING_AGE = 15;
    private static final int MAX_AGE = 250;
    private static final double BREEDING_PROBABILITY = 0.08;
    private static final int MAX_LITTER_SIZE = 5;
    private static final int EAGLE_FOOD_VALUE = 25;

    private int direction = 1;
    private int swoopCountdown = 9;
    private int hunger = 0;

    private boolean isSwooped = false;

    public Eagle(Field field, int xLocation){
        super(field, new Location(0, xLocation));
    }

    public void poopoo(){
        super.incrementAge();
        hunger++;

        if (isSwooped){
            setLocation(new Location(0, getLocation().getCol()));
            isSwooped = false;
        }

        if(isAlive()) {
            swoopCountdown--;
            if (swoopCountdown <= 0){
                //Do swoop
                Location foodLoc = findFood();

                if (foodLoc == null){
                    swoopCountdown = 9;
                    return;
                }
                else{
                    setLocation(foodLoc);
                    isSwooped = true;
                }

                swoopCountdown = 9;
            }
            else{
                Location newLoc = new Location(0, getLocation().getCol() + direction);
                if (newLoc.getCol() >= getField().getWidth() || newLoc.getCol() < 0){
                    direction *= -1;
                    newLoc = new Location(0, getLocation().getCol() + direction + direction);
                }
                setLocation(newLoc);
            }
        }
        else{
            setDead();
        }
    }

    @Override
    public void act(List<Animal> newAnimals) {
        Location loc = getLocation();
        poopoo();
        if (loc.equals(getLocation())){
            System.out.println("ERROR EAGLE STUCKY WUCKY");
        }
    }

    private Location findFood()
    {
        int currentCol = getLocation().getCol();
        Field field = getField();
        LinkedList<Location> cells = new LinkedList<Location>();
        for (int i = 1; i < 46; i++){
            cells.addLast(new Location(i, currentCol));
        }

        Iterator<Location> it = cells.iterator();
        while(it.hasNext()) {
            Location where = it.next();
            Object animal = field.getObjectAt(where);
            if(animal instanceof Rabbit) {
                ((Animal)animal).setDead();
                this.hunger = 0;
                return where;
            }
        }
        return null;
    }

    @Override
    protected int getBreedingAge() {
        return BREEDING_AGE;
    }

    @Override
    protected int getMaxAge() {
        return MAX_AGE;
    }

    @Override
    protected double getBreedingProbability() {
        return BREEDING_PROBABILITY;
    }

    @Override
    protected int getMaxLitterSize() {
        return MAX_LITTER_SIZE;
    }
    
    @Override
    protected boolean isAlive(){
        return super.isAlive() && super.getAge() <= MAX_AGE && this.hunger <= EAGLE_FOOD_VALUE;
    }
}