public abstract class Animal {
  protected String name;
  protected String food_type;
  protected int development_stage;
  protected int workers;
  protected int numOfTotalAnimals;
  
  public String getName() {
    return name;
  }
  public String getFoodType() {
    return food_type;
  }
  public int getDevelopmentStage() {
    return development_stage;
  }
  public int getWorkers() {
    return workers;
  }
  public int getNumOfTotalAnimals() {
    return numOfTotalAnimals;
  }
  
  public void setName(String name) {
    this.name = name;
  }
  public void setFoodType(String food) {
    food_type = food;
  }
  public void setDevelopmentStage(int development) {
    development_stage = development;
  }
  public void setWorkers(int workers) {
    this.workers = workers;
  }
  public void setNumOfTotalAnimals(int animals) {
    numOfTotalAnimals = animals;
  }
  
  abstract int getTemp();
  abstract int lbsOfFood();
  abstract int toString();
}