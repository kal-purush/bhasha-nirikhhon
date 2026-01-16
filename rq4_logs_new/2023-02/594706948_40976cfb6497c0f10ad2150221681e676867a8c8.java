package src.main.java.homework4;

public class Cat extends Animals{

    private static int catCounter = 0;
    private String name;

    @Override
    public void run(int runLength) {
        while (true) {

            if (runLength > 200 || runLength < 1) {
                System.out.println("Cats cant run more then 200m or less then 1m!");
            } else {
                System.out.println(this.name + " runs " + runLength + " meters.");
            }
            break;

        }
    }

    public Cat (String name){
        this.name = name;
        catCounter++;
    }


    public static int getCatCounter() {
        return catCounter;
    }

    public String getName() {
        return name;
    }
}