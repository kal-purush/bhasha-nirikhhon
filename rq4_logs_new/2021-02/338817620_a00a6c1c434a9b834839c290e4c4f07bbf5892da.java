package check.numbers;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class ValidatorTests {

    @Test
    public void validateNumber_1() {
        String str = "27831234567";
        Number num = new Number(str);
        Validator val = new Validator(num);
        val.validateNumber();
        assertEquals(val.getNumber().getNumber(), "27831234567");
        assertFalse(val.getNumber().isModified());
        assertTrue(val.getNumber().isCorrect());
        assertEquals(val.getNumber().getAnnotation(), "The number is correct!");
    }

    @Test
    public void validateNumber_2() {
        String str = "281234567";
        Number num = new Number(str);
        Validator val = new Validator(num);
        val.validateNumber();
        assertEquals(val.getNumber().getNumber(), "27831234567");
        assertTrue(val.getNumber().isModified());
        assertTrue(val.getNumber().isCorrect());
        assertEquals(val.getNumber().getAnnotation(), "The number is correct!");
    }

    @Test
    public void validateNumber_3() {
        String str = "731234567";
        Number num = new Number(str);
        Validator val = new Validator(num);
        val.validateNumber();
        assertEquals(val.getNumber().getNumber(), "27831234567");
        assertTrue(val.getNumber().isModified());
        assertTrue(val.getNumber().isCorrect());
        assertEquals(val.getNumber().getAnnotation(), "The number is correct!");
    }

    @Test
    public void validateNumber_4() {
        String str = "71234567";
        Number num = new Number(str);
        Validator val = new Validator(num);
        val.validateNumber();
        assertEquals(val.getNumber().getNumber(), "27831234567");
        assertTrue(val.getNumber().isModified());
        assertTrue(val.getNumber().isCorrect());
        assertEquals(val.getNumber().getAnnotation(), "The number is correct!");
    }

    @Test
    public void validateNumber_5() {
        String str = "231234567";
        Number num = new Number(str);
        Validator val = new Validator(num);
        val.validateNumber();
        assertEquals(val.getNumber().getNumber(), "27831234567");
        assertTrue(val.getNumber().isModified());
        assertTrue(val.getNumber().isCorrect());
        assertEquals(val.getNumber().getAnnotation(), "The number is correct!");
    }

    @Test
    public void validateNumber_6() {
        String str = "781234567";
        Number num = new Number(str);
        Validator val = new Validator(num);
        val.validateNumber();
        assertEquals(val.getNumber().getNumber(), "27831234567");
        assertTrue(val.getNumber().isModified());
        assertTrue(val.getNumber().isCorrect());
        assertEquals(val.getNumber().getAnnotation(), "The number is correct!");
    }

    @Test
    public void validateNumber_7() {
        String str = "1234567";
        Number num = new Number(str);
        Validator val = new Validator(num);
        val.validateNumber();
        assertEquals(val.getNumber().getNumber(), "27831234567");
        assertTrue(val.getNumber().isModified());
        assertTrue(val.getNumber().isCorrect());
        assertEquals(val.getNumber().getAnnotation(), "The number is correct!");
    }

    @Test
    public void validateNumbers_1() {
        Number a0 = new Number("2712345678");
        Number a1 = new Number("8312345678");
        Number a2 = new Number("12345678");
        Number a3 = new Number("12345");
        Number a4 = new Number("2783");
        Number a5 = new Number("_DELETED_1488454802");
        Number a6 = new Number("1234567_DELETED_1486284887");
        Number a7 = new Number("ciao");
        Number a8 = new Number("");
        Number a9 = new Number("2783123456a");
        List<Number> lst = new ArrayList<>();
        lst.add(a0);
        lst.add(a1);
        lst.add(a2);
        lst.add(a3);
        lst.add(a4);
        lst.add(a5);
        lst.add(a6);
        lst.add(a7);
        lst.add(a8);
        lst.add(a9);
        Validator val = new Validator(lst);
        val.validateNumbers();
        List<Number> finalList = val.getNumbers();
        for (int i=0; i<5; i++){
            assertEquals(finalList.get(i).getAnnotation(), "The numbers must be 11 digits!");

        }
        for (int i=5; i<10; i++){
            assertEquals(finalList.get(i).getAnnotation(), "The number must contain only digits!");
        }
    }
}