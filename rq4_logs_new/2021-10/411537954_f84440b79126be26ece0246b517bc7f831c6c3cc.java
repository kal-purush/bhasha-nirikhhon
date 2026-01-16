package cooper.parser;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import cooper.parser.CommandParser;
import cooper.exceptions.InvalidArgumentException;
import cooper.exceptions.UnrecognisedCommandException;

public class ParserTest {


    @BeforeAll
    static void setUpCommandParser() {
    }

    @Test
    @Order(1)
    void test_UnrecognisedCommandException() {
        assertThrows(UnrecognisedCommandException.class, () -> {
            CommandParser.parse("foo 123");
        });

        assertThrows(UnrecognisedCommandException.class, () -> {
            CommandParser.parse("$%^&&");
        });
    }

    void test_InvalidArgumentException() {
        assertThrows(InvalidArgumentException.class, () -> {
            CommandParser.parse("add $%^&");
        });

        assertThrows(InvalidArgumentException.class, () -> {
            CommandParser.parse("meetings $%^&");
        });

    }
}