package com.pet.pet_funeral.domain.pet_funeral.mapper;

import com.pet.pet_funeral.domain.pet_funeral.dto.PetFuneralRequest;
import com.pet.pet_funeral.domain.pet_funeral.model.PetFuneral;
import org.mapstruct.Mapper;
import org.mapstruct.factory.Mappers;

@Mapper(componentModel = "spring")
public interface PetFuneralMapper {

    PetFuneralMapper INSTANCE = Mappers.getMapper(PetFuneralMapper.class);

    // 장례식장 저장 요청 DTO -> 장례식장 엔티티
    PetFuneral toMessageBodyDto(PetFuneralRequest requestDto);
}