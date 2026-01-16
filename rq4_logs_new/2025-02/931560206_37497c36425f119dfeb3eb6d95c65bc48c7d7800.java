package id.ac.ui.cs.advprog.eshop.controller;

import org.apache.hc.core5.http.HttpStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.json.AutoConfigureJsonTesters;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.json.JacksonTester;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.context.ContextConfiguration;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import id.ac.ui.cs.advprog.eshop.model.Car;
import id.ac.ui.cs.advprog.eshop.service.CarService;

@AutoConfigureJsonTesters
@ContextConfiguration(classes = CarController.class)
@WebMvcTest(CarController.class)
public class CarControllerTest {
    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JacksonTester<Car> jsonCar;

    @MockBean
    private CarService carService;

    Car car;

    @BeforeEach
    void setUp() {
        this.car = new Car();
        this.car.setCarId("eb558e9f-1c39-460e-8860-71af6af63bd6");
        this.car.setCarName("Toyota Avanza");
        this.car.setCarColor("Silver");
        this.car.setCarQuantity(10);
    }

    @Test
    void canGetCreateCar() throws Exception {
        MockHttpServletResponse response = mockMvc.perform(
                get("/car/createCar")
        ).andReturn().getResponse();

        assert (response.getStatus() == HttpStatus.SC_OK);
    }

    @Test
    void canPostCreateCar() throws Exception {
        when(carService.create(any(Car.class))).thenReturn(car);

        String json = jsonCar.write(car).getJson();
        MockHttpServletResponse response = mockMvc.perform(
                post("/car/createCar")
                        .contentType("application/json")
                        .content(json != null ? json : "")
        ).andReturn().getResponse();

        assert (response.getStatus() == HttpStatus.SC_MOVED_TEMPORARILY);
        assert (response.getRedirectedUrl().equals("/car/listCar"));
    }

    @Test
    void canGetCarList() throws Exception {
        List<Car> carList = new ArrayList<>();
        carList.add(car);

        when(carService.findAll()).thenReturn(carList);

        MockHttpServletResponse response = mockMvc.perform(
                get("/car/listCar")
        ).andReturn().getResponse();

        assert (response.getStatus() == HttpStatus.SC_OK);
    }

    @Test
    void canGetEditCar() throws Exception {
        when(carService.findById(car.getCarId())).thenReturn(car);

        MockHttpServletResponse response = mockMvc.perform(
                get("/car/editCar/" + car.getCarId())
        ).andReturn().getResponse();

        assert (response.getStatus() == HttpStatus.SC_OK);
    }

    @Test
    void canPostEditCar() throws Exception {
        Car updatedCar = new Car();
        updatedCar.setCarId(car.getCarId());
        updatedCar.setCarName("Toyota Avanza Veloz");
        updatedCar.setCarColor("Black");
        updatedCar.setCarQuantity(15);

        Mockito.doNothing().when(carService).update(eq(car.getCarId()), any(Car.class));

        String json = jsonCar.write(updatedCar).getJson();
        MockHttpServletResponse response = mockMvc.perform(
                post("/car/editCar")
                        .contentType("application/json")
                        .content(json != null ? json : "")
        ).andReturn().getResponse();

        assert (response.getStatus() == HttpStatus.SC_MOVED_TEMPORARILY);
        assert (response.getRedirectedUrl().equals("/car/listCar"));
    }

    @Test
    void canDeleteCar() throws Exception {
        Mockito.doNothing().when(carService).deleteCarById(car.getCarId());

        MockHttpServletResponse response = mockMvc.perform(
                post("/car/deleteCar")
                        .param("carId", car.getCarId())
        ).andReturn().getResponse();

        assert (response.getStatus() == HttpStatus.SC_MOVED_TEMPORARILY);
        assert (response.getRedirectedUrl().equals("/car/listCar"));
    }
}