package test.ua.goit;

import main.ua.goit.Converter;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ConverterTest {

    @Test
    public void toBinary() {
        String number1 = "100";
        String number2 = "1F";
        Integer system1 = 10;
        Integer system2 = 16;
        String expected1 = "1100100";
        String expected2 = "11111";

        String result1 = Converter.toBinary(number1, system1);
        String result2 = Converter.toBinary(number2, system2);

        assertEquals(expected1, result1);
        assertEquals(expected2, result2);
    }

    @Test
    public void fromBinary() {
        String binaryNumber1 = "1010";
        String binaryNumber2 = "11111";
        Integer system1 = 10;
        Integer system2 = 16;
        String expected1 = "10";
        String expected2 = "1f";

        String result1 = Converter.fromBinary(binaryNumber1, system1);
        String result2 = Converter.fromBinary(binaryNumber2, system2);

        assertEquals(expected1, result1);
        assertEquals(expected2, result2);
    }

    @Test
    public void toDecimal() {
        String number1 = "1001";
        String number2 = "1f";
        Integer system1 = 2;
        Integer system2 = 16;
        Integer expected1 = 9;
        Integer expected2 = 31;

        Integer result1 = Converter.toDecimal(number1, system1);
        Integer result2 = Converter.toDecimal(number2, system2);

        assertEquals(expected1, result1);
        assertEquals(expected2, result2);
    }

    @Test
    public void decimalTo() {
        Integer decimalNumber1 = 31;
        Integer decimalNumber2 = 10;
        Integer system1 = 16;
        Integer system2 = 2;
        String expected1 = "1f";
        String expected2 = "1010";

        String result1 = Converter.decimalTo(decimalNumber1, system1);
        String result2 = Converter.decimalTo(decimalNumber2, system2);

        assertEquals(expected1, result1);
        assertEquals(expected2, result2);
    }
}