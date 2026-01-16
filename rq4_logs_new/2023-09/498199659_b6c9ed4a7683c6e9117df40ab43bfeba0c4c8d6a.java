package com.ssu.commerce.book.dto.mapper;

import com.ssu.commerce.book.dto.param.ChangeBookParamDto;
import com.ssu.commerce.book.dto.param.query.UpdateBookParamDto;
import org.mapstruct.Mapper;
import org.mapstruct.NullValueMappingStrategy;
import org.mapstruct.ReportingPolicy;
import org.mapstruct.factory.Mappers;

@Mapper(
        componentModel = "spring",
        unmappedTargetPolicy = ReportingPolicy.ERROR,
        typeConversionPolicy = ReportingPolicy.ERROR,
        nullValueMappingStrategy = NullValueMappingStrategy.RETURN_DEFAULT
)
public interface UpdateBookParamDtoMapper {

    UpdateBookParamDtoMapper INSTANCE = Mappers.getMapper(UpdateBookParamDtoMapper.class);

    UpdateBookParamDto map(ChangeBookParamDto dto);
}