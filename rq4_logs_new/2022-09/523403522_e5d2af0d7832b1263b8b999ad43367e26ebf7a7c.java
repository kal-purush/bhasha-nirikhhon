package ru.practicum.shareit.requests.dto;

import ru.practicum.shareit.item.model.Item;
import ru.practicum.shareit.requests.ItemRequest;
import ru.practicum.shareit.user.User;

import java.time.LocalDateTime;

public class ItemRequestMapper {

    public static ItemRequest fromDtoToRequest(ItemRequestDto requestDto, Long id, User requestor, LocalDateTime created) {
        return ItemRequest.builder()
                .id(id)
                .description(requestDto.getDescription())
                .requestor(requestor)
                .created(created)
                .build();
    }

    public static ItemRequestDto toDtoRequest(ItemRequest itemRequest) {
        return ItemRequestDto.builder()
                .id(itemRequest.getId())
                .description(itemRequest.getDescription())
                .created(itemRequest.getCreated())
                .items(null)
                .build();
    }
}