package com.ssu.commerce.book.dto.mapper;

import com.ssu.commerce.book.dto.param.RentalBookRequestDto;
import com.ssu.commerce.order.grpc.RentalBookRequest;
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
public interface RentalBookRequestDtoMapper {
    RentalBookRequestDtoMapper INSTANCE = Mappers.getMapper(RentalBookRequestDtoMapper.class);

    RentalBookRequestDto map(RentalBookRequest request);
}