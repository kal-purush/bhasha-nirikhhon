/**
 * Created by admin on 2015-09-23.
 */
public class CounterModel implements CounterInterface {

    public CounterModel (int modulus){
        if(modulus > 1){
            this.modulus = modulus;
        }else{
            throw new IllegalArgumentException("For input: " + modulus);
        }
    }
    public CounterModel (){
        modulus = 10;
    }

    private int modulus;
    private int x = 0;

    public void increment(){
        x = ++x % modulus;
    }

    public void decrement(){
        if(0 != x){
            x--;
        }else{
            x = 59;
        }
    }
    public boolean equals(Object o){
        if(o instanceof CounterModel){
            CounterModel cm = (CounterModel)o;
            if(((CounterModel) o).getValue() == getValue() && ((CounterModel) o).getModulus() == getModulus()){
                return true;
            }
        }
        return false;
    }

    public String toString(){
        return "The counter is at: " + getValue() + ". And the cap is at: " + getModulus();
    }

    public int getModulus() {
        return modulus;
    }
    public void reset(){
        x = 0;
    }
    public int getValue(){
        return x;
    }

}