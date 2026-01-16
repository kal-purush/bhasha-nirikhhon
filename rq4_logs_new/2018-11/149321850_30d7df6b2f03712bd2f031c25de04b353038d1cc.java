package hu.uni.miskolc.iit;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class UserTest {

    private String lastName = "Nagy";
    private String firstName = "János";
    private String password = "psw";
    private String neptunCode = "ABC123";
    private String email = "email@email.com";
    private long phoneNumber = 367031;
    User user;

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Before
    public void before() {
        user = new User(lastName, firstName, password, neptunCode, email, phoneNumber);
    }

    @Test
    public void testConstructor(){
        try{
            User user = new User(lastName, firstName, password, neptunCode, email, phoneNumber);
        }
        catch(Exception ex){
            fail(ex.getMessage());
        }

    }

    @Test
    public void testSetLastName(){
        String lastName = "Pál";
        user.setLastName(lastName);
        assertEquals(lastName, user.getLastName());
    }

    @Test
    public void testSetFirstName(){
        String firstName = "Pál";
        user.setFirstName(firstName);
        assertEquals(firstName, user.getFirstName());
    }

    @Test
    public void testSetPassword(){
        String password = "Pál";
        user.setPassword(password);
        assertEquals(password, user.getPassword());
    }

    @Test
    public void testSetNeptunCode(){
        String code = "ABC123";
        user.setNeptunCode(code);
        assertEquals(code, user.getNeptunCode());
    }

    @Test
    public void testSetEmail(){
        String email = "Pál";
        user.setEmail(email);
        assertEquals(email, user.getEmail());
    }

    @Test
    public void testSetPhoneNumber(){
        long num = 111111;
        user.setPhoneNumber(num);
        assertEquals(num, user.getPhoneNumber());
    }

    @Test
    public void testPhoneNumberWithWrongInput(){
        exceptionRule.expect(IllegalArgumentException.class);
        user.setPhoneNumber(123);
    }

    @Test (expected = IllegalArgumentException.class)
    public void testNeptunCodeWithWrongInput(){
        user.setNeptunCode("ABC");
    }

    @Test (expected = IllegalArgumentException.class)
    public void testLastNameWithWrongInput(){
        user.setLastName("");
    }

    @Test (expected = IllegalArgumentException.class)
    public void testFirstNameWithWrongInput(){
        user.setFirstName("");
    }

    @Test (expected = IllegalArgumentException.class)
    public void testEmailWithWrongInput(){
        user.setEmail("");
    }

    @Test (expected = IllegalArgumentException.class)
    public void testPasswordWithWrongInput(){
        user.setPassword("");
    }
}