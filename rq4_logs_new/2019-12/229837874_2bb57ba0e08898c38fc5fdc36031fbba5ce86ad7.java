public class FizzBuzz {
    public String fizzBuzz(int number) {
        // fizzbuzz logic here
        if (number % 3==0 && number % 5==0){
            return "Fizzbuzz";
        }
        else if (number % 3==0){
            return "Fizz";
        }
         else if (number % 5==0){
            return "Buzz";
         }
         return String.format("Output is: %s", number);
    }
}