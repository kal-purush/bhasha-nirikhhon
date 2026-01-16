package ru.clevertec.carshowroom.service.car;

import ru.clevertec.carshowroom.dto.car.AddCarDTO;
import ru.clevertec.carshowroom.dto.car.OutputCarDTO;
import ru.clevertec.carshowroom.dto.car.UpdateCarDTO;
import ru.clevertec.carshowroom.dto.carshowroom.OutputCarShowroomDTO;

import java.util.List;

public interface CarService {

    AddCarDTO addCar(AddCarDTO addCarDTO);

    List<OutputCarDTO> findAll();

    OutputCarDTO findById(Long id);

    UpdateCarDTO update(UpdateCarDTO updatedCarDTO);

    boolean deleteById(Long id);

    void assignCarToShowroom(OutputCarDTO outputCarDTO, OutputCarShowroomDTO outputCarShowroomDTO);

    List<OutputCarDTO> findCarsByFilters(String brand, String category, Integer minPrice, Integer maxPrice);

    List<OutputCarDTO> findCarsSortedByPrice(boolean ascending);

    List<OutputCarDTO> findCarsWithPagination(int pageNumber, int pageSize);
}