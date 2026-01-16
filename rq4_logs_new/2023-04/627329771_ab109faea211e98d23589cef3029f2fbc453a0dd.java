package org.flowershop.domain;

import org.flowershop.domain.products.Decoration;
import org.flowershop.exceptions.NegativeValueException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class DecorationTest {

    @DisplayName("Adding new stock")
    @Test
    public void updateDecorationStock() {
        // arrange
        Decoration decoration = new Decoration("D001", "Enredadera de madera", 26.75, "Madera");

        // act
        try {
            decoration.setStock(5);
            decoration.updateStock(10);
        } catch (NegativeValueException e) {
            System.out.println(e.getMessage());
        }

        // assert
        assertEquals("D001", decoration.getRef());
        assertEquals(15, decoration.getStock());
    }

    @DisplayName("Addition of a negative stock value")
    @Test
    public void setDecorationStockWithNegativeValue() {
        Decoration decoration = new Decoration("D001", "Enredadera de madera", 26.75, "Madera");

        // assert
        assertEquals("D001", decoration.getRef());
        try {
            decoration.setStock(-5);
            // The exception must be thrown
            fail("Expected NegativeValueException to be thrown.");
        } catch (NegativeValueException e) {
            assertEquals("Stock cannot be negative.", e.getMessage());
            assertEquals(0, decoration.getStock());
        }
    }

    @DisplayName("Updating price with a negative value.")
    @Test
    public void updatePriceWithNegativeValue() {
        Decoration decoration = new Decoration("D001", "Enredadera de madera", 26.75, "Madera");

        // assert
        assertEquals("D001", decoration.getRef());
        try {
            decoration.setPrice(-5);
            // The exception must be thrown
            fail("Expected NegativeValueException to be thrown.");
        } catch (NegativeValueException e) {
            assertEquals("The price cannot be negative.", e.getMessage());
            assertEquals(26.75, decoration.getPrice());
        }
    }

    @DisplayName("Decoration constructor takes the price in absolute value.")
    @Test
    public void settingTreeWithNegativePrice() {
        Decoration decoration = new Decoration("D001", "Enredadera de madera", -26.75, "Madera");

        // assert
        assertEquals(26.75, decoration.getPrice());
    }

}