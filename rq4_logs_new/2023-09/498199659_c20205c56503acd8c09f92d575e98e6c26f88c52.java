package com.ssu.commerce.book.dto.mapper;

import com.ssu.commerce.book.dto.param.CompleteRentalBookRequestDto;
import com.ssu.commerce.order.grpc.CompleteRentalBookRequest;
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
public interface CompleteRentalBookRequestDtoMapper {
    CompleteRentalBookRequestDtoMapper INSTANCE = Mappers.getMapper(CompleteRentalBookRequestDtoMapper.class);

    CompleteRentalBookRequestDto map(CompleteRentalBookRequest request);
}