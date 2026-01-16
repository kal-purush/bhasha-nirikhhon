package pl.maks.carrental.convertor;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import pl.maks.carrental.controller.productDTO.CarDTO;
import pl.maks.carrental.repository.model.Car;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class CarConverterTest {
    private static final Integer ID = 11;
    private static final String BRAND = "TestBrand";
    private static final String CAR_CLASS = "TestCarClass";
    private static final String FUEL = "TestFuel";
    private static final String MODEL = "TestModel";
    private static final Double PRICE_PER_DAY = 100.0;

    private final ParkingConverter parkingConverter = mock(ParkingConverter.class);
    private final CarConverter target = new CarConverter(parkingConverter);

    @Test
    @DisplayName("Should convert CarDTO to Car")
    void shouldConvertCarDtoToEntity() {
        //given
        CarDTO car = carDTO();
        Car expected = entityCar();

        //when
        Car actual = target.convertToEntity(car);

        //then
        assertThat(actual.getId()).isEqualTo(expected.getId());
        assertThat(actual.getBrand()).isEqualTo(expected.getBrand());
        assertThat(actual.getCarClass()).isEqualTo(expected.getCarClass());
        assertThat(actual.getFuel()).isEqualTo(expected.getFuel());
        assertThat(actual.getModel()).isEqualTo(expected.getModel());
        assertThat(actual.getPricePerDay()).isEqualTo(expected.getPricePerDay());
    }

    @Test
    @DisplayName("Should convert Car to DTO")
    void shouldConvertCarToDto() {
        //given
        Car car = entityCar();
        CarDTO expected = carDTO();

        //when
        CarDTO actual = target.convertToDto(car);

        //then
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    @DisplayName("Should convert Collection<Cars> to List<CarDTO>")
    void shouldConvertCollectionCarsToListCarsDTO() {
        //given
        Collection<Car> cars = List.of(entityCar());
        List<CarDTO> expected = List.of(carDTO());

        //when
        List<CarDTO> actual = target.convertToDto(cars);

        //then
        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    private Car entityCar() {
        Car car = new Car();
        car.setId(ID);
        car.setBrand(BRAND);
        car.setCarClass(CAR_CLASS);
        car.setFuel(FUEL);
        car.setModel(MODEL);
        car.setPricePerDay(BigDecimal.valueOf(PRICE_PER_DAY));
        return car;
    }

    private CarDTO carDTO() {
        CarDTO carDTO = new CarDTO();
        carDTO.setId(ID);
        carDTO.setBrand(BRAND);
        carDTO.setCarClass(CAR_CLASS);
        carDTO.setFuel(FUEL);
        carDTO.setModel(MODEL);
        carDTO.setPricePerDay(BigDecimal.valueOf(PRICE_PER_DAY));
        return carDTO;
    }
}