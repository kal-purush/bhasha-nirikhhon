package week7.task1;

public class ExpressionTest {
    public static void main(String[] args) {
            Numeral ten = new Numeral(10);
            Numeral two = new Numeral(2);
            Numeral nine = new Numeral(9);
            Numeral three = new Numeral(3);

            Square square = new Square(ten);
            Subtraction subtraction = new Subtraction(square, nine);
            Multiplication multiplication = new Multiplication(two, three);
            Addition addition = new Addition(subtraction, multiplication);
            Square square1 = new Square(addition);
            System.out.println(square1.evaluate());

            Numeral zero = new Numeral();
            Division division = new Division(ten,zero);
            try{
                System.out.println(division.evaluate());
            }catch (ArithmeticException e){
                System.out.println("Lỗi chia cho 0");
            }
    }
}