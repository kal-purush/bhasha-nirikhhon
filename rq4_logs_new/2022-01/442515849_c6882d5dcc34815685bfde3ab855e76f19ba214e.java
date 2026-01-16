package data.Test;

import data.AccredNumb;
import data.Exceptions.NullObjectException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AcreedNumbTest {

    @Test
    public void NullExceptionTest(){
        NullObjectException thrown = assertThrows(NullObjectException.class,() -> new AccredNumb(null));
        assertTrue(thrown.getMessage().equals("Objecte sense instanciar"));
    }
}