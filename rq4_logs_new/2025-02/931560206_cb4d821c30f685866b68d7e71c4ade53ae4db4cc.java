package id.ac.ui.cs.advprog.eshop.controller;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
public class HomeControllerTest {
    @InjectMocks
    HomeController homeController = new HomeController();

    @Test
    void testHomePage() {
        assertEquals("homepage", homeController.homePage());
    }
}