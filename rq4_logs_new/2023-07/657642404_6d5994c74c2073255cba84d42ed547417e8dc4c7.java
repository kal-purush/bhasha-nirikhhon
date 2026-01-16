import java.util.List;
import java.util.Arrays;

public class Prob_412 {
    public List<String> fizzBuzz(int n) {
        String[] arr = new String[n];
        for (int i=0; i<n; i++){
            if ((i+1) % 15 == 0){
                arr[i] = "FizzBuzz";
            }
            else if ((i+1) % 3 ==0){
                arr[i] = "Fizz";
            }
            else if ((i+1) % 5 ==0){
                arr[i] = "Buzz";
            }
            else {
                arr[i] = ""+(i+1);
            }
        }
        return Arrays.asList(arr);
    }
}