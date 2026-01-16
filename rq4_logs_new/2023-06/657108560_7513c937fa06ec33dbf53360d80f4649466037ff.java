package study;

import org.junit.jupiter.api.Test;

import java.util.Objects;

class StringCalculatorTest {

    @Test
    void splitStringBySpace() {
        int before = 0;
        String value = "2 + 3 + 5 + 5 * 2";
        String operation = "";
        String[] values = value.split(" ");
        for (String v: values) {
            switch (v) {
                case "+":
                    operation = "+";
                    break;
                case "*":
                    operation = "*";
                    break;
                default:
                    int current = Integer.parseInt(v);
                    if (operation == "*") {
                        before *= current;
                        break;
                    }
                    if (operation == "+") {
                        before += current;
                        break;
                    }
                    before = current;
            }
        }
        System.out.println(before);
    }
}

class StringCalculator {

}