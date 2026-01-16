// FizzBuzz
// 1から整数を数える
// 3で割り切れる = Fizz
// 5で割り切れる = Buzz
// 3でも5でも割り切れる = FizzBuzz
public class Main {
    public static void main(String[] args) throws Exception {
        String output;
        for(int i =1; i <= 30; i++) {
            if(i % 3 == 0 && i % 5 == 0){
                output = i + ":FizzBuzz";
            } else if(i % 3 == 0) {
                output = i + ":Fizz";
            } else if(i % 5 == 0) {
                output = i + ":Buzz";
            } else {
                output = String.valueOf(i);
            }
            System.out.println(output);
        }
    }
}