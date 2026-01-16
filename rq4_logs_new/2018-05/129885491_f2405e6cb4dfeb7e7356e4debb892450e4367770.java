public class Methods2 {
    public static float calculateAverage(int firstPrice, int secondPrice){
        return float (firstPrice + secondPrice)/2
    }

    float average1= calculateAverage(6,5);
    float average2= calculateAverage(6,6);
    float average3= calculateAverage(6,7);

    switch(average1 > average2 & average3){
        case:
            System.out.println("Average1 is the higher + " average1);
    }

}