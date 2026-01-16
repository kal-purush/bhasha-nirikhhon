package lesson_16_Functional_Programming.homework;

import java.util.Random;
import java.util.function.Function;
import java.util.function.Supplier;

public class HomeWork_16_Main {
    public static void main(String[] args) {

// 2)
        MathOperation mathOperation = new MathOperation() {
            @Override
            public int operate(int a, int b) {
                return a + b;
            }
        };

        getOutput(mathOperation.operate(5, 3));

// 3)
        StringManipulator stringManipulator = (s) -> s.toUpperCase();
        getOutput(stringManipulator.toUppercase("hillel java pro"));

// 4)
        getOutput(StringListProcessor
                .countUpperCase(" p P _aa R a@a$ D ii G -m M #@^& "));

// 5)
        Function<String, Integer> function = StringListProcessor::countUpperCase;
        getOutput("Capital letters in word=VeRiTaS: "
                + function.apply("VeRiTaS"));

// 6)
        getOutput(RandomNumberGenerator.generateRandomNumber(1,100));

// 7)
        Supplier<Integer> supplier = () -> new Random().nextInt(100);
        getOutput(supplier.get());
    }

    static void getOutput(Integer i) {
        System.out.println(i);
    }

    static void getOutput(String s) {
        System.out.println(s);
    }
}