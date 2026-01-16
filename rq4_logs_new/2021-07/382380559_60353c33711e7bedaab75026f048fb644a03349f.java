package guru.qa;

import org.junit.jupiter.api.*;

public class FirstTest {

    @BeforeAll
    static void beforeAll(){
        System.out.println("this is before all method!");
    }

    @BeforeEach
    void setUp() {
     System.out.println("this is before each method!");
    }

    @AfterEach
    void tearDown() {
        System.out.println("this is after each method!");
    }

    @Test
    void firstTest() {
        System.out.println("this is the first @test!");
        Assertions.assertTrue(true);
    }

    @Test
    void secondTest() {
        System.out.println("this is the second @test!");
        Assertions.assertTrue(true);
    }
}