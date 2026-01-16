package ru.clevertec.carshowroom.controller;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import ru.clevertec.carshowroom.dto.carshowroom.CarShowroomRequest;
import ru.clevertec.carshowroom.dto.carshowroom.CarShowroomResponse;
import ru.clevertec.carshowroom.dto.carshowroom.UpdateCarShowroomRequest;
import ru.clevertec.carshowroom.service.CarShowroomService;

import java.util.List;

@RestController
@Validated
@RequiredArgsConstructor
@RequestMapping("/car-showrooms")
public class CarShowroomController {
    private final CarShowroomService carShowroomService;

    @GetMapping
    @ResponseStatus(HttpStatus.OK)
    public List<CarShowroomResponse> getAllCarShowrooms() {
        return carShowroomService.findAll();
    }

    @GetMapping("/{id}")
    @ResponseStatus(HttpStatus.OK)
    public CarShowroomResponse getCarShowRoomById(@PathVariable("id") @NotBlank Long id) {
        return carShowroomService.findById(id);
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public CarShowroomRequest addCarShowroom(@RequestBody @Valid CarShowroomRequest carShowroomRequest) {
        return carShowroomService.addCarShowroom(carShowroomRequest);
    }

    @PutMapping()
    @ResponseStatus(HttpStatus.CREATED)
    public UpdateCarShowroomRequest update(@RequestBody @Valid UpdateCarShowroomRequest updateCarShowroomRequest) {
        return carShowroomService.update(updateCarShowroomRequest);
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteById(@PathVariable("id") @Valid @NotBlank Long id) {
        carShowroomService.deleteById(id);
    }
}