package ie.atu.microservices;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class UserTest {
    User Joe;

    @BeforeEach
    void setUp() {

    }

    @Test
    void testName_success() {
        User Shan = new User("Shannon",12345678L,370,"shannongf74@gmail.com");
        assertEquals("Shannon",Shan.getName());
    }

    @Test
    void testName_fail(){
        Exception e_message = assertThrows(IllegalArgumentException.class, () -> Joe = new User("JT", 12345678L, 700, "shannongf74@gmail.com"));
        assertEquals("Name must be 3 characters or longer",e_message.getMessage());
    }
    @AfterAll
    static void afterAll() {

    }
}