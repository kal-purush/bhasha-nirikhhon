import java.util.ArrayList;

public class ArrayListWithSum extends ArrayList<Integer> implements ListWithSum {

    public int sum(){
        int sum = 0;
        for(int i = 0; i<size(); i++){
            sum += get(i);
        }
        return sum;
    }

}