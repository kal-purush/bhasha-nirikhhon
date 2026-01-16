package ru.practicum.shareit.request.mapper;

import org.mapstruct.Mapper;
import ru.practicum.shareit.item.mapper.ItemMapper;
import ru.practicum.shareit.request.dto.ItemRequestDto;
import ru.practicum.shareit.request.dto.NewItemRequestDto;
import ru.practicum.shareit.request.model.ItemRequest;

import java.util.List;

@Mapper(componentModel = "spring", uses = ItemMapper.class)
public interface ItemRequestMapper {

    ItemRequest toModel(NewItemRequestDto addItemRequestDto);

    ItemRequestDto toDto(ItemRequest itemRequest);

    List<ItemRequestDto> toDtoList(List<ItemRequest> requests);
}