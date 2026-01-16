package variablesprogramme;

public class VariablesAndComments {
    public static void main(String[] args) {
    // Declare the variables

    // Thruth based variables

        boolean isUsed;
    boolean hasBaterry;
    boolean isAndroid;
    boolean isStolen;
    boolean isBlocked;
    boolean isCharged;

    // Char related
    char condition;

    // Numeric-Related
    byte numberOfSimCards;
    byte randomAccessMemory;

    short productionYear;

    int numberOfOwners;

    long id;
    float operatingSystemVersion;
    double price;

    //Initialize an Iphone

     isUsed = false;
     hasBaterry = true;
     isAndroid = false;
     isStolen = false;
     isCharged = true;

     condition = 'A';

     numberOfSimCards = 2;
     randomAccessMemory = 4;
     numberOfOwners = 0;
     productionYear = 2023;
     id = 4555454554L;
     operatingSystemVersion = 16.4F;
     price = 34500.5;

     System.out.println("The phone is charged" + isCharged);

     System.out.println("The phone is priced" + price);


    }

}