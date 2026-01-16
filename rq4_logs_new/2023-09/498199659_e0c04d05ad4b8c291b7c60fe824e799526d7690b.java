package com.ssu.commerce.book.dto.mapper;

import com.ssu.commerce.book.dto.param.RollBackBookRequestDto;
import com.ssu.commerce.order.grpc.RollBackBookRequest;
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
public interface RollBackBookRequestDtoMapper {
    RollBackBookRequestDtoMapper INSTANCE = Mappers.getMapper(RollBackBookRequestDtoMapper.class);

    RollBackBookRequestDto map(RollBackBookRequest request);
}