package ru.clevertec.carshowroom.service.car.impl;

import lombok.RequiredArgsConstructor;
import ru.clevertec.carshowroom.dto.car.AddCarDTO;
import ru.clevertec.carshowroom.dto.car.OutputCarDTO;
import ru.clevertec.carshowroom.dto.car.UpdateCarDTO;
import ru.clevertec.carshowroom.dto.carshowroom.OutputCarShowroomDTO;
import ru.clevertec.carshowroom.entity.CarEntity;
import ru.clevertec.carshowroom.exception.CarNotFoundException;
import ru.clevertec.carshowroom.mapper.CarMapper;
import ru.clevertec.carshowroom.mapper.CarShowroomMapper;
import ru.clevertec.carshowroom.repository.car.CarRepository;
import ru.clevertec.carshowroom.service.car.CarService;

import java.util.List;
import java.util.Optional;

@RequiredArgsConstructor
public class CarServiceImpl implements CarService {
    private final CarRepository carRepository;
    private final CarMapper carMapper;
    private final CarShowroomMapper carShowroomMapper;

    @Override
    public AddCarDTO addCar(AddCarDTO addCarDTO) {
        CarEntity carEntity = carRepository.addCar(carMapper.toCarEntity(addCarDTO));
        return carMapper.toAddCarDTO(carEntity);
    }

    @Override
    public List<OutputCarDTO> findAll() {
        return carMapper.toOutputCarDTOs(carRepository.findAll());
    }

    @Override
    public OutputCarDTO findById(Long id) {
        Optional<CarEntity> optionalCarEntity = carRepository.findCarById(id);
        if (optionalCarEntity.isPresent()) {
            return carMapper.toOutputCarDTO(optionalCarEntity.get());
        } else {
            throw CarNotFoundException.byId(id);
        }
    }

    @Override
    public UpdateCarDTO update(UpdateCarDTO updatedCarDTO) {
        Optional<CarEntity> optionalCarEntity = carRepository.update(carMapper.toCarEntity(updatedCarDTO));
        if (optionalCarEntity.isPresent()) {
            return carMapper.toUpdateCarDTO(optionalCarEntity.get());
        } else {
            throw CarNotFoundException.byId(updatedCarDTO.getId());
        }
    }

    @Override
    public boolean deleteById(Long id) {
        if (carRepository.deleteById(id)) {
            return true;
        } else {
            throw CarNotFoundException.byId(id);
        }
    }

    @Override
    public void assignCarToShowroom(OutputCarDTO outputCarDTO, OutputCarShowroomDTO outputCarShowroomDTO) {
        CarEntity carEntity = carMapper.toCarEntity(outputCarDTO);
        System.out.println(carEntity);
        carRepository.assignCarToShowroom(carMapper.toCarEntity(outputCarDTO),
                carShowroomMapper.toCarShowroomEntity(outputCarShowroomDTO));
    }

    @Override
    public List<OutputCarDTO> findCarsByFilters(String brand, String category, Integer minPrice, Integer maxPrice) {
        List<CarEntity> carEntities = carRepository.findCarsByFilters(brand, category, minPrice, maxPrice);
        return carMapper.toOutputCarDTOs(carEntities);
    }

    @Override
    public List<OutputCarDTO> findCarsSortedByPrice(boolean ascending) {
        List<CarEntity> carEntities = carRepository.findCarsSortedByPrice(ascending);
        return carMapper.toOutputCarDTOs(carEntities);
    }

    @Override
    public List<OutputCarDTO> findCarsWithPagination(int pageNumber, int pageSize) {
        List<CarEntity> carEntities = carRepository.findCarsWithPagination(pageNumber, pageSize);
        return carMapper.toOutputCarDTOs(carEntities);
    }
}