/**
 * Created by admin on 2015-09-30.
 */
public class TestCounter {
    public static void main (String [] args){
        //Skapar 2 CounterModels
        CounterModel cm1 = new CounterModel();
        CounterModel cm2 = new CounterModel(5);
        //Skapar de klasser som ärver utav CounterModel
        FastCounter cm3 = new FastCounter(5, 2);
        ChainedCounterModel cm4 = new ChainedCounterModel(5,10,cm1);

        //Skriver ut och ändrar värden på do olika CounterModel objekten.
        System.out.println("Is cm1 equal to cm2? \t\t\t\t" + cm1.equals(cm2) + " \t---Should give False.");

        //Ökar både cm1 och cm2 med 10 så att båda står på 0
        for(int i = 0; 10 < i++;){
            cm1.increment();
            cm2.increment();
        }
        System.out.println("Is cm1 at the same count as cm2?\t" + (cm1.getValue() == cm2.getValue()) + " \t---Should give True.");


        System.out.println("Is cm2 equal to cm3? \t\t\t\t" + cm2.equals(cm3) + " \t---Should give False.");
        //Minskar cm2 med 3 så att den stannar på 2 (5-3)
        cm2.decrement();
        cm2.decrement();
        cm2.decrement();
        //Ökar cm3 med 1 (*2) så att den stannar på 2;
        cm3.upMany();

        System.out.println("Is cm2 at the same count as cm3?\t" + (cm2.getValue() == cm3.getValue()) + " \t---Should give True.");

        //Ökar ChainedCounterModel till dess modulus så att cm1 (countermodelen som den får som argument) ökar med 1
        for(int i = 0; 5 >i++;){
            cm4.increment();
        }
        System.out.println("Did cm1s count increase by one?\t\t" + (cm1.getValue() == 1) + " \t---Should give True.");
    }
}