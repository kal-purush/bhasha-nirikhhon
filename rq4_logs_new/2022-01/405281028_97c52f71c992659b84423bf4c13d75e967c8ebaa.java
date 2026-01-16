package src.exceptions.division;

/*
1.Create the class Numbers which contains an attribute of type int .
2.Add a method to it - divideBy , which will take one int parameter.
3.Create the class TestNumber , and in the main method call divideBy by passing the value 0 (zero).
Try to run the project.
4.Handle the exception and display an appropriate message.
5.Edit the method divideBy and throw IllegalArgumentException when the input parameter is 0.
 */
public class Numbers {
    private int number;

    public Numbers(int number) {
        this.number = number;
    }

    public double divideBy(int numberByDivide) {
        if (numberByDivide == 0){
            IllegalArgumentException  iAE =  new IllegalArgumentException("It is not possible to divide by 0!!");
            throw iAE;
        }

        return number/numberByDivide;
    }
}