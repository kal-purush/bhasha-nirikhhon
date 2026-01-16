package org.openapitools.mappers;

import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingConstants;
import org.openapitools.dto.ComentarioDTO.ComentarioRequestDto;
import org.openapitools.dto.ComentarioDTO.ComentarioResponseDto;
import org.openapitools.model.Comentario;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING)
public interface ComentarioMapper {

    @Mapping(target = "id", ignore = true)
    @Mapping(target = "fechaCreacion", ignore = true)
    Comentario toEntity(ComentarioRequestDto dto);

    @Mapping(source = "fechaCreacion", target = "fechacreacion")
    ComentarioResponseDto toDto(Comentario comentario);
}