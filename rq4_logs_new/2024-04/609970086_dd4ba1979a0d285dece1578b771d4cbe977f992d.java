package com.bearAndPupperCo.sangenWrestlingApp.APIUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class ValidationUtilsTest {

    @InjectMocks
    ValidationUtils validationUtils;

    @Test
    void validateOrderDirectionASCTest() {
        //Arrange
        String orderDirection = "ASC";

        //Act
        Boolean result = validationUtils.validateOrderDirection(orderDirection);

        //Assert
        assertTrue(result);
    }

    @Test
    void validateOrderDirectionDESCTest() {
        //Arrange
        String orderDirection = "DESC";

        //Act
        Boolean result = validationUtils.validateOrderDirection(orderDirection);

        //Assert
        assertTrue(result);
    }

    @Test
    void validateOrderDirectionErrorTest() {
        //Arrange
        String orderDirection = "WRONG_ORDER_DIRECTION";

        //Act
        Boolean result = validationUtils.validateOrderDirection(orderDirection);

        //Assert
        assertFalse(result);
    }
}