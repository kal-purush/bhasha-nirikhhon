package no.hvl.dat108;

import static org.junit.Assert.*;

import org.junit.Test;

import no.hvl.dat108.Validator;

public class ValidatorTest {

    /*
     * A valid username should be at least 4 characters and contain
     * only letters and numbers, but not starting with a number.
     */
    @Test
    public void validUsernamesShouldBeOk() {
        assertTrue(Validator.isValidUsername("aaaa"));
        assertTrue(Validator.isValidUsername("aAaA"));
        assertTrue(Validator.isValidUsername("abc4"));
        assertTrue(Validator.isValidUsername("A6789b"));
        assertTrue(Validator.isValidUsername("A6789b"));
    }
    
    @Test
    public void norwegianLettersShouldBeAllowed() {
        assertTrue(Validator.isValidUsername("������"));
    }
    
    @Test
    public void shortUsernamesShouldNotBeOk() {
        assertFalse(Validator.isValidUsername(null));
        assertFalse(Validator.isValidUsername(""));
        assertFalse(Validator.isValidUsername("a"));
        assertFalse(Validator.isValidUsername("ABC"));
    }
    
    @Test
    public void usernamesWithIllegalCharsShouldNotBeOk() {
        assertFalse(Validator.isValidUsername("a-bcd"));
        assertFalse(Validator.isValidUsername("a@bcd"));
    }
    
    @Test
    public void usernamesStartingWithANumberShouldNotBeOk() {
        assertFalse(Validator.isValidUsername("1abcde"));
        assertFalse(Validator.isValidUsername("0ABCDE"));
    }
    
    

}