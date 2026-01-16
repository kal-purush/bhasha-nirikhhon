package ru.clevertec.carshowroom.controller;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import ru.clevertec.carshowroom.dto.car.CarRequest;
import ru.clevertec.carshowroom.dto.car.CarResponse;
import ru.clevertec.carshowroom.dto.car.UpdateCarRequest;
import ru.clevertec.carshowroom.service.CarService;
import ru.clevertec.carshowroom.service.CarShowroomService;


import java.util.List;


@RestController
@Validated
@RequiredArgsConstructor
@RequestMapping("/cars")
public class CarController {
    private final CarService carService;
    private final CarShowroomService carShowroomService;

    @GetMapping
    @ResponseStatus(HttpStatus.OK)
    public List<CarResponse> getAllCars() {
        return carService.findAll();
    }

    @GetMapping("/{id}")
    @ResponseStatus(HttpStatus.OK)
    public CarResponse getCarById(@PathVariable("id") Long id) { //@NotBlank
        return carService.findById(id);
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public CarRequest addCar(@RequestBody @Valid CarRequest carRequest) {
        return carService.addCar(carRequest);
    }

    @PutMapping()
    @ResponseStatus(HttpStatus.CREATED)
    public UpdateCarRequest update(@RequestBody @Valid UpdateCarRequest updateCarRequest) {
        return carService.update(updateCarRequest);
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteById(@PathVariable("id") @Valid @NotBlank Long id) {
        carService.deleteById(id);
    }

    @PatchMapping("/{carId}/assign-showroom/{showRoomId}")
    @ResponseStatus(HttpStatus.CREATED)
    public void assignCarToShowroom(@PathVariable("carId") Long carId, @PathVariable("showRoomId") Long showRoomId) {
        carService.assignCarToShowroom(carService.findById(carId), carShowroomService.findById(showRoomId));
    }

    @GetMapping("/filter")
    @ResponseStatus(HttpStatus.OK)
    public List<CarResponse> findCarsByFilters(@RequestParam(name = "brand", required = false) String brand,
                                               @RequestParam(name = "minPrice", required = false) Integer minPrice,
                                               @RequestParam(name = "maxPrice", required = false) Integer maxPrice) {
        return carService.findCarsByFilters(brand, minPrice, maxPrice);
    }

    @GetMapping("/sort-by-price")
    @ResponseStatus(HttpStatus.OK)
    public List<CarResponse> findCarsSortedByPrice(@RequestParam(value = "ascending") boolean ascending) {
        return carService.findCarsSortedByPrice(ascending);
    }

    @GetMapping("/pagination")
    @ResponseStatus(HttpStatus.OK)
    public List<CarResponse> findCarsWithPagination(
            @RequestParam("pageNumber") int pageNumber,
            @RequestParam("pageSize") int pageSize) {
        return carService.findCarsWithPagination(pageNumber, pageSize);
    }
}