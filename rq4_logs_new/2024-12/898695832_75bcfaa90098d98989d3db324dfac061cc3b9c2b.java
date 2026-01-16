package ru.clevertec.carshowroom.repository.car;

import ru.clevertec.carshowroom.entity.CarEntity;
import ru.clevertec.carshowroom.entity.CarShowroomEntity;

import java.util.List;
import java.util.Optional;

public interface CarRepository {

    CarEntity addCar(CarEntity carEntity);

    List<CarEntity> findAll();

    Optional<CarEntity> findCarById(Long id);

    Optional<CarEntity> update(CarEntity updatedCarEntity);

    boolean deleteById(Long id);

    void assignCarToShowroom(CarEntity carEntity, CarShowroomEntity carShowroomEntity);

    List<CarEntity> findCarsByFilters(String brand, String category, Integer minPrice, Integer maxPrice);
    List<CarEntity> findCarsSortedByPrice(boolean ascending);
    List<CarEntity> findCarsWithPagination(int pageNumber, int pageSize);
}