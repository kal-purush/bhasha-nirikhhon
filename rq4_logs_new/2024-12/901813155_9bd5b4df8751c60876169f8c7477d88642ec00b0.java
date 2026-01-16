import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class Test2 {

@Test
void testGetName() {
    Menu menu = new Menu() {
    };
    menu.setName("Burger");
    assertEquals("Burger", menu.getName());
}

@Test
void testGetWeight() {
    Menu menu = new Menu() {
    };
    menu.setWeight(200);
    assertEquals(200, menu.getWeight());
}

@Test
void testGetPrice() {
    Menu menu = new Menu() {
    };
    menu.setPrice(10);
    assertEquals(10, menu.getPrice());
}

@Test
void testSetName() {
    Menu menu = new Menu() {
    };
    menu.setName("Pizza");
    assertEquals("Pizza", menu.getName());
}

@Test
void testSetWeight() {
    Menu menu = new Menu() {
    };
    menu.setWeight(300);
    assertEquals(300, menu.getWeight());
}

@Test
void testSetPrice() {
    Menu menu = new Menu() {
    };
    menu.setPrice(15);
    assertEquals(15, menu.getPrice());
}
}
