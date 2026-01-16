package ru.practicum.shareit.request;

import ru.practicum.shareit.request.dto.ItemRequestDto;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ItemRequestMapper {

    public static ItemRequestDto objectToDto(ItemRequest object) {
        return ItemRequestDto.builder()
                .id(object.getId())
                .requestorId(object.getRequestor().getId())
                .description(object.getDescription())
                .created(object.getCreated())
                .items(new ArrayList<>())
                .build();
    }


    public static ItemRequest dtoToObject(ItemRequestDto dto) {
        return ItemRequest.builder()
                .description(dto.getDescription())
                .build();
    }

    public static List<ItemRequestDto> objectToDto(List<ItemRequest> objects) {
        return objects.stream()
                .map(ItemRequestMapper::objectToDto)
                .collect(Collectors.toList());
    }
}