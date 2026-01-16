package ru.clevertec.carshowroom.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import ru.clevertec.carshowroom.dto.car.AddCarDTO;
import ru.clevertec.carshowroom.dto.car.OutputCarDTO;
import ru.clevertec.carshowroom.dto.car.UpdateCarDTO;
import ru.clevertec.carshowroom.entity.CarEntity;

import java.util.List;

@Mapper
public interface CarMapper {
    @Mapping(source = "categoryId", target = "categoryEntity.id")
    CarEntity toCarEntity(AddCarDTO addCarDTO);

    @Mapping(source = "categoryEntity.id", target = "categoryId")
    AddCarDTO toAddCarDTO(CarEntity carEntity);

    @Mapping(source = "categoryId", target = "categoryEntity.id")
    @Mapping(source = "categoryName", target = "categoryEntity.name")
    @Mapping(source = "showroomTitle", target = "carShowroomEntity.title")
    @Mapping(source = "showroomId", target = "carShowroomEntity.id")
    @Mapping(source = "clients", target = "clientEntities")
    @Mapping(source = "reviews", target = "reviewEntities")
    CarEntity toCarEntity(OutputCarDTO outputCarDTO);

    @Mapping(source = "categoryEntity.id", target = "categoryId")
    @Mapping(source = "categoryEntity.name", target = "categoryName")
    @Mapping(source = "carShowroomEntity.title", target = "showroomTitle")
    @Mapping(source = "carShowroomEntity.id", target = "showroomId") //////////
    @Mapping(source = "clientEntities", target = "clients")
    @Mapping(source = "reviewEntities", target = "reviews")
    OutputCarDTO toOutputCarDTO(CarEntity carEntity);

    List<OutputCarDTO> toOutputCarDTOs(List<CarEntity> carEntities);

    @Mapping(source = "categoryId", target = "categoryEntity.id")
    CarEntity toCarEntity(UpdateCarDTO updateCarDTO);

    @Mapping(source = "categoryEntity.id", target = "categoryId")
    UpdateCarDTO toUpdateCarDTO(CarEntity carEntity);

    UpdateCarDTO toUpdateCarDTO(OutputCarDTO outputCarDTO);
}
