package cooper.verification;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class VerifierTest {

    static Verifier verifier;

    @BeforeAll
    static void setUpVerifier() {
        verifier = new Verifier();
    }

    @Test
    @Order(1)
    void verify_properInputFirstLoginAttempt_loginFailed() {
        String input = "login Topias as admin";
        SignInDetails actual = verifier.verify(input);

        SignInDetails expected = new SignInDetails("Topias", UserRole.ADMIN);
        assertTrue(hasSameAttributeValuesAs(actual, expected));
        assertFalse(verifier.isSuccessfullySignedIn());
    }

    @Test
    @Order(2)
    void verify_properInputRegisterUser_registrationSuccessful() {
        String input = "register Martin as admin";
        SignInDetails actual = verifier.verify(input);

        SignInDetails expected = new SignInDetails("Martin", UserRole.ADMIN);
        assertTrue(hasSameAttributeValuesAs(actual, expected));
        assertFalse(verifier.isSuccessfullySignedIn());
    }

    @Test
    @Order(3)
    void verify_properInputLoginAfterRegister_loginSuccessful() {
        String input = "register Martin as admin";
        SignInDetails actual = verifier.verify(input);

        input = "login Martin as admin";
        actual = verifier.verify(input);

        SignInDetails expected = new SignInDetails("Martin", UserRole.ADMIN);
        assertTrue(hasSameAttributeValuesAs(actual, expected));
        assertTrue(verifier.isSuccessfullySignedIn());

        assertTrue(verifier.getRegisteredUsers().containsKey("Martin"));
        assertEquals(verifier.getRegisteredUsers().get("Martin"), UserRole.ADMIN);
    }

    @Test
    @Order(4)
    void verify_properInputLoginWrongRole_loginFailed() {
        String input = "login Martin as employee";
        SignInDetails actual = verifier.verify(input);

        SignInDetails expected = new SignInDetails("Martin", UserRole.EMPLOYEE);
        assertTrue(hasSameAttributeValuesAs(actual, expected));
        assertFalse(verifier.isSuccessfullySignedIn());
    }

    @Test
    @Order(5)
    void verify_improperInputEmptyUsernameField_failure() {
        String input = "login as admin";
        SignInDetails signInDetails = verifier.verify(input);

        assertNull(signInDetails);
        assertFalse(verifier.isSuccessfullySignedIn());
    }

    @Test
    @Order(6)
    void verify_improperInputEmptyRoleField_failure() {
        String input = "login Sebastian as";
        SignInDetails signInDetails = verifier.verify(input);

        assertNull(signInDetails);
        assertFalse(verifier.isSuccessfullySignedIn());
    }

    @Test
    @Order(7)
    void verify_emptyInputString_failure() {
        String input = "";
        SignInDetails signInDetails = verifier.verify(input);

        assertNull(signInDetails);
        assertFalse(verifier.isSuccessfullySignedIn());
    }

    @Test
    @Order(8)
    void verify_emptyArguments_failure() {
        String input = "register";
        SignInDetails signInDetails = verifier.verify(input);

        assertNull(signInDetails);
        assertFalse(verifier.isSuccessfullySignedIn());
    }

    private static boolean hasSameAttributeValuesAs(SignInDetails actual, SignInDetails expected) {
        boolean hasSameUsername = actual.getUsername().equals(expected.getUsername());
        boolean hasSameRole = actual.getUserRole().equals(expected.getUserRole());
        return (hasSameUsername && hasSameRole);
    }
}