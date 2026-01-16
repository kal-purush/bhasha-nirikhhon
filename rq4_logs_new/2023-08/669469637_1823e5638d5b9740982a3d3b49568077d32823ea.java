package pl.maks.carrental.validator;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import pl.maks.carrental.controller.productDTO.CarDTO;
import pl.maks.carrental.controller.productDTO.ParkingDTO;
import pl.maks.carrental.exception.CarRentalValidationException;
import pl.maks.carrental.repository.CarRepository;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class CarValidatorTest {
    private final CarRepository carRepository = mock(CarRepository.class);
    private final CarValidator target = new CarValidator(carRepository);
    private final ParkingDTO parkingDTO = mock(ParkingDTO.class);
    //private final CarDTO carDTO = mock(CarDTO.class);

    @Test
    @DisplayName("Validation Error should not be thrown when input client is valid")
    void shouldNotThrow_whenCarIsValid() {
        //given
        CarDTO valid = validCar();

        //when
        assertDoesNotThrow(() -> target.carValidation(valid));

        //then
    }

    @Test
    @DisplayName("Validation Error should be thrown when FieldName is blank")
    void shouldThrow_whenFieldNameIsBlank() {
        //given
        CarDTO emptyFieldName = emptyFieldName();
        String expectedMessage = "fieldName is blank";

        //when
        CarRentalValidationException carRentalValidationException = assertThrows(CarRentalValidationException.class, () -> target.carValidation(emptyFieldName));

        //then
        assertThat(carRentalValidationException.getViolations()).contains(expectedMessage);
    }

    @Test
    @DisplayName("Validation Error should be thrown when price is zero")
    void shouldThrow_whenPriceIsNull() {
        //given
        CarDTO nullPriceCar = nullPriceCar();
        String expectedMessage = "price per day cannot be null or more than zero";

        //when
        CarRentalValidationException carRentalValidationException = assertThrows(CarRentalValidationException.class, () -> target.carValidation(nullPriceCar));

        //then
        assertThat(carRentalValidationException.getViolations()).contains(expectedMessage);
    }

    @Test
    @DisplayName("Validation Error should be thrown when Parking id is null")
    void shouldThrow_whenParkingIdIsNull() {
        //given
        CarDTO nullParkingIdCar = nullParkingIdCar();
        String expectedMessage = "Parking id is null";

        //when
        CarRentalValidationException carRentalValidationException = assertThrows(CarRentalValidationException.class, () -> target.carValidation(nullParkingIdCar));

        //then
        assertThat(carRentalValidationException.getViolations()).contains(expectedMessage);
    }
        private CarDTO validCar () {
            CarDTO carDTO = new CarDTO();
            carDTO.setBrand("TestBrand");
            carDTO.setCarClass("TestCarClass");
            carDTO.setFuel("TestFuel");
            carDTO.setModel("TestModel");
            carDTO.setParking(parkingDTO);
            carDTO.setPricePerDay(BigDecimal.valueOf(150.0));
            return carDTO;
        }
        private CarDTO emptyFieldName () {
            CarDTO carDTO = new CarDTO();
            carDTO.setBrand("TestBrand");
            carDTO.setCarClass("TestCarClass");
            carDTO.setFuel("TestFuel");
            carDTO.setModel("TestModel");
            carDTO.setParking(parkingDTO);
            carDTO.setPricePerDay(BigDecimal.valueOf(150.0));
            return carDTO;

        }
        private CarDTO nullPriceCar () {
            CarDTO carDTO = new CarDTO();
            carDTO.setBrand("TestBrand");
            carDTO.setCarClass("TestCarClass");
            carDTO.setFuel("TestFuel");
            carDTO.setModel("TestModel");
            carDTO.setParking(parkingDTO);
            carDTO.setPricePerDay(new BigDecimal(0));
            return carDTO;
        }
    private CarDTO nullParkingIdCar() {
        CarDTO carDTO = new CarDTO();
        carDTO.setBrand("TestBrand");
        carDTO.setCarClass("TestCarClass");
        carDTO.setFuel("TestFuel");
        carDTO.setModel("TestModel");
        carDTO.setParking(new ParkingDTO());
        carDTO.setPricePerDay(BigDecimal.valueOf(150.0));
        return carDTO;
    }
}