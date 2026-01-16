package agh.ics.oop;
import agh.ics.oop.Simulation.AbstractSimulationEngine;
import agh.ics.oop.gui.DataFile;
import java.util.*;


public abstract class AbstractWorldMap implements IWorldMap{

    protected HashMap<Vector2d, Grass> grassHashMap = new HashMap<Vector2d,Grass>(); // contains all of the grass on the map
    protected HashMap<int[], Integer> genotypeDominantTable = new HashMap<int[], Integer>(); // contains genomes and teh quantity of animals that have them
    protected int[] genotypeDominant = new int[32]; // contains genome dominant
    protected List<Animal> animalToKill = new ArrayList<>();    // contains animals that are supposed to be removed from map
    protected int height;   // maps height
    protected int width;    // maps width
    protected double jungleRatio;
    protected int jungleWidth;
    protected int jungleHeight;
    protected Vector2d jungleUpperRightCorner;
    protected Vector2d jungleLowerLeftCorner;
    protected StatisticsData observer = new StatisticsData(); // object that updates and contains statistical data from this map
    protected int plantEnergy;
    protected int moveEnergy;
    protected int startEnergy;
    protected DataFile dataFile; // object that creates and updates csv file for this map
    private final Random random = new Random();

    // hashmap that contains all animals on the map. List containing animals with the same locations are the elements of this map and locations are the keys
    public Map<Vector2d, List<Animal>> animals = new HashMap<>();

    // SortedSet that contains location when new animal can be born
    private final SortedSet<Vector2d> animalToCreate = new TreeSet<>(new Comparator<Vector2d>() {
        @Override
        public int compare(Vector2d o1, Vector2d o2) {
            return o1.compareByXY(o2);
        }
    });
    // SortedSet that contains grass that is supposed to be eaten
    private final SortedSet<Vector2d> grassToEat = new TreeSet<>(new Comparator<Vector2d>() {
        @Override
        public int compare(Vector2d o1, Vector2d o2) {
            return o1.compareByXY(o2);
        }
    });
    // SertedSet that contains locations that are currently occupied
    private final SortedSet<Vector2d> occupiedFields = new TreeSet<>(new Comparator<Vector2d>() {
        @Override
        public int compare(Vector2d o1, Vector2d o2) {
            return o1.compareByXY(o2);
        }
    });

    public AbstractWorldMap(int width, int height, double jungleRatio,int startEnergy, int moveEnergy, int plantEnergy) {
        this.height = height;
        this.width = width;
        this.jungleRatio = jungleRatio;
        this.createJungle(); // creates jungleUpperRightCorner and jungleLowerLeftCorner
        this.plantEnergy = plantEnergy;
        this.startEnergy = startEnergy;
        this.moveEnergy = moveEnergy;
    }

    // grass is being eaten and removed from map
    public void eatGrass(){
        for (Vector2d vector2d : grassToEat) {
            int counter = 0;
            Animal strongest = (Animal) objectAt(vector2d); // objectAt returns the strongest animal (the one with most energy)
            // loop is checking if there is more animals with the same amount of energy in this location and counts them
            for(Animal animal : animals.get(vector2d)) {
                if(animal.getEnergy() > strongest.getEnergy()){
                    strongest = animal;
                    counter = 0;
                }
                else if(animal.getEnergy() == strongest.getEnergy()){
                    counter += 1;
                }
            }
            // of there is more grass is being shared
            if(counter>1){
                for(Animal animal : animals.get(vector2d)){
                    if(animal.getEnergy() == strongest.getEnergy()){
                        animal.eat(counter);
                    }
                }
            }
            else{strongest.eat(1);}

            // grass is being removed from map and the object that stores statistical data is being informed about the change
            grassHashMap.remove(vector2d);
            observer.updateQuantityOfGrass(-1);
            observer.updateTotalEnergy(plantEnergy);
        }
        grassToEat.clear();
    }

    public void createNewAnimals(AbstractSimulationEngine engine){
        for (Vector2d vector2d : animalToCreate) {
            if(animals.get(vector2d).size()<2){continue;} // if there is less than two animals in this location nothing happens
            // looking for two strongest animals on this field
            Animal strongest = (Animal) objectAt(vector2d);
            animals.get(vector2d).remove(strongest);
            Animal secondStrongest = (Animal) objectAt(vector2d);
            animals.get(vector2d).add(strongest);

            // if both of them can have baby, then the baby is created
            if(strongest.canHaveBaby() && secondStrongest.canHaveBaby()) {
                double actualEnergy = strongest.getEnergy()/4 + secondStrongest.getEnergy()/4; // energy of the new animal
                engine.addAnimal(strongest,secondStrongest,vector2d,actualEnergy); // engine is being informed that it is supposed to add new animals
                //object that stores statistical data is being informed about the change
                observer.updateTotalEnergy(-actualEnergy);
                observer.updateQuantityOfChildrenOfLivingAnimals(2);
            }
        }
        animalToCreate.clear();
    }

    // updating changes
    public void positionChanged(Vector2d oldPosition, Vector2d newPosition, Animal animal) {
        // if animal moved to another location, some of lists and maps are being updated
        if(!oldPosition.equals(newPosition)) {
            animals.get(oldPosition).remove(animal);
            if(animals.get(oldPosition).size() == 0){
                occupiedFields.remove(oldPosition);
            }
            if(!animals.containsKey(newPosition)){
                List<Animal> newList = new ArrayList<>();
                animals.put(newPosition,newList);
            }
            animals.get(newPosition).add(animal);
            occupiedFields.add(newPosition);
        }
        // if two animals are on the same field baby may be born (field is added to list)
        if (animals.get(newPosition).size() == 2) {
            animalToCreate.add(newPosition);
        }
        // if animal moved to place with grass, grass is supposed to be eaten
        if (grassHashMap.containsKey(newPosition)) {
            grassToEat.add(newPosition);
        }
        //object that stores statistical data is being informed about the change
        observer.updateTotalEnergy(-moveEnergy);

    }
    public void addAnimalToKill(Animal animal){animalToKill.add(animal);} // allows animal to inform map that it doesn't have energy to move anymore

    // dead bodies are being removed from map
    public void getRidOfBodies(AbstractSimulationEngine engine){
        for (Animal animal : animalToKill) {
            // if animal didn't eat something it is being removed from lists
            if (!animal.getIsAlive()) {
                animals.get(animal.getLocation()).remove(animal);
                // if this animal was the only was on this position the occupiedFields list is being updated
                if(animals.get(animal.getLocation()).size() == 0){
                    occupiedFields.remove(animal.getLocation());
                }
                engine.removeBodies(animal); // informs engine to remove animal from his list

                //object that stores statistical data is being informed about the change
                observer.updateQuantityOfLivingAnimals(-1);
                observer.updateQuantityOfDeadAnimals(1);
                observer.updateTotalLifespan(animal.getLifespan());
                observer.updateTotalEnergy(-animal.getEnergy());
                observer.updateQuantityOfChildrenOfLivingAnimals(-animal.getQuantityOfChildren());

                // hashmap with genomes is being informed about the change
                int x = genotypeDominantTable.get(animal.getGenotype());
                x -= 1;
                genotypeDominantTable.remove(animal.getGenotype());
                genotypeDominantTable.put(animal.getGenotype(),x);
                // if this animals genome was the same as dominant genome, map starts searching for new dominant
                if(Arrays.equals(animal.getGenotype(), genotypeDominant)){
                    searchingForNewDominant();
                }
            }
        }
        animalToKill.clear();
    }
    // it checks every available genome if its new dominant
    private void searchingForNewDominant(){
        for(Vector2d position: occupiedFields){
            for(Animal animal: animals.get(position)){
                if(genotypeDominantTable.get(genotypeDominant) < genotypeDominantTable.get(animal.getGenotype())){
                    genotypeDominant = animal.getGenotype();
                }
            }
        }
    }

    public List<Animal> searchingForAnimalsWithDominantGenome(){
        List<Animal> animalWithDominantGenome = new ArrayList<>();
        for(Vector2d position: occupiedFields){
            for(Animal animal: animals.get(position)){
                if(Arrays.equals(animal.getGenotype(), genotypeDominant)){
                    animalWithDominantGenome.add(animal);
                }
            }
        }
        return animalWithDominantGenome;
    }

    // this method places animal at map
    public void place(Animal animal){
        // if hashmap doesnt contains tihs location new list is being made
        if(!animals.containsKey(animal.getLocation())){
            List<Animal> newList = new ArrayList<>();
            animals.put(animal.getLocation(),newList);
        }
        animals.get(animal.getLocation()).add(animal);
        occupiedFields.add(animal.getLocation());

        // if it is first animal on the map then dominant is set for his genome
        if(observer.getQuantityOfLivingAnimals() == 1){
            genotypeDominant = animal.getGenotype();
        }
        // genome dominant map and dominant are being updated
        if(genotypeDominantTable.containsKey(animal.getGenotype())){
            int x;
            x = genotypeDominantTable.get(animal.getGenotype());
            x += 1;
            genotypeDominantTable.remove(animal.getGenotype());
            genotypeDominantTable.put(animal.getGenotype(),x);

            if(x > genotypeDominantTable.get(genotypeDominant)){
                genotypeDominant = animal.getGenotype();
            }
        }
        else{
            genotypeDominantTable.put(animal.getGenotype(),1);
        }

        //object that stores statistical data is being informed about the change
        observer.updateQuantityOfLivingAnimals(1);
        observer.updateTotalEnergy(animal.getEnergy());

    }

    public boolean isOccupied(Vector2d position) {
        if(grassHashMap.get(position) !=null){
            return true;
        }
        else if(!animals.containsKey(position)){
            return false;
        }
        else return animals.get(position).size() >= 1;}

    // it returns the strongest animal or if there is no animal, it returns grass or nothing
    public IMapElement objectAt(Vector2d position) {
        if(animals.containsKey(position) && animals.get(position).size()>=1){
            Animal strongest = animals.get(position).get(0);
            for(Animal animal: animals.get(position)){
                if(animal.getEnergy()>strongest.getEnergy()){
                    strongest = animal;
                }
            }
            return strongest;
        }
        else return grassHashMap.getOrDefault(position, null);
    }
    // creates corners of the jungle
    public void createJungle(){
        this.jungleHeight = (int)(height * jungleRatio);
        this.jungleWidth = (int)(width * jungleRatio);
        int x1 = (int) ((width-jungleWidth)/2);
        int y1 = (int) ((height-jungleHeight)/2);
        this.jungleUpperRightCorner = new Vector2d(x1+jungleWidth-1,y1+jungleHeight-1);
        this.jungleLowerLeftCorner = new Vector2d(x1,y1);

    }
    // this method checks if object belongs to jungle
    public boolean belongToJungle(int x, int y){return x >= jungleLowerLeftCorner.getX() && x <= jungleUpperRightCorner.getX() && y >= jungleLowerLeftCorner.getY() && y <= jungleUpperRightCorner.getY();}

    public void addGrassToSavana(){
        int x = random.nextInt(width);
        int y = random.nextInt(height);
        int i= 0;
        while((isOccupied(new Vector2d(x,y)) || belongToJungle(x,y)) && i<100)
        {
            x = random.nextInt(width);
            y = random.nextInt(height);
            i++;
        }
        Grass grass = new Grass(new Vector2d(x,y));
        grassHashMap.put(grass.getLocation(),grass);
        observer.updateQuantityOfGrass(1);
    }

    public void addGrassToJungle(){
        if(this.jungleWidth==0 || this.jungleHeight==0){return;}
        int i= 0;
        int x = random.nextInt(jungleWidth);
        int y = random.nextInt(jungleHeight);
        while(isOccupied(new Vector2d(x+jungleLowerLeftCorner.getX(),y+jungleLowerLeftCorner.getY())) && i<50 ) // przesunięcie o wektor polozenia jungli
        {
            i += 1;
            x = random.nextInt(jungleWidth);
            y = random.nextInt(jungleHeight);
        }
        if(i!=100){
            x = x+jungleLowerLeftCorner.getX();
            y = y+jungleLowerLeftCorner.getY();
            Grass grass = new Grass(new Vector2d(x,y));
            grass.setInJungle(true);
            grassHashMap.put(new Vector2d(x,y),grass);
            observer.updateQuantityOfGrass(1);
        }

    }
    // this methods enable to get data from this object
    public int getWidth() {return width;}
    public int getHeight() {return height;}
    public DataFile getDataFile(){
        return dataFile;
    }
    public StatisticsData getStatisticsData(){
        return  observer;
    }
    public int[] getGenotypeDominant(){
        if(observer.getQuantityOfLivingAnimals()>0){
            return genotypeDominant;
        }
        else {return null;
        }
    }

}