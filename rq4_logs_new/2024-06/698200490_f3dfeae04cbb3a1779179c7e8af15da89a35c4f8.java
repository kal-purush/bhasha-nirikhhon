package org.petmarket.users.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.petmarket.config.MapperConfig;
import org.petmarket.users.dto.BlackListEntityResponseDto;
import org.petmarket.users.dto.BlackListResponseDto;

import java.util.List;

@Mapper(config = MapperConfig.class, uses = {UserMapper.class})
public interface BlackListMapper {
    @Mapping(target = "userResponseDto", source = "user")
    BlackListResponseDto mapEntityToDto(BlackListEntityResponseDto entity);

    List<BlackListResponseDto> mapEntityToBlackListDto(List<BlackListEntityResponseDto> entities);
}