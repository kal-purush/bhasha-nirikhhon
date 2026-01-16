package com.botdatamessage.mapper;

import com.botdatamessage.model.Domain;
import com.botdatamessage.model.DomainUp;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;

@Mapper(componentModel = "spring")
public interface DomainMapper {
    @Mapping(target = "user", ignore = true)
    Domain toEntity(DomainUp domainUp);
}