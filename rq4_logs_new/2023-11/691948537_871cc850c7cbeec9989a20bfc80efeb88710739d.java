package ru.practicum.shareit.request.controller;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import ru.practicum.shareit.request.dto.ItemRequestDto;
import ru.practicum.shareit.request.service.ItemRequestService;
import ru.practicum.shareit.util.Logging;

import java.util.List;

@RestController
@Data
@Slf4j
@RequestMapping(path = "/requests")
public class ItemRequestController {
    private final ItemRequestService itemRequestService;

    @GetMapping
    public List<ItemRequestDto> findAllByRequester(@RequestHeader("X-Sharer-User-Id") long requesterId) {
        Logging.logInfoIncomingRequest(log, "GET /requests", requesterId);
        return itemRequestService.findAllByRequester(requesterId);
    }

    @GetMapping("/{requestId}")
    public ItemRequestDto retrieve(@PathVariable long requestId, @RequestHeader("X-Sharer-User-Id") long userId) {
        Logging.logInfoIncomingRequest(log, "GET /requests/{requestId}", requestId, userId);
        return itemRequestService.retrieve(requestId, userId);
    }

    @GetMapping("/all")
    public List<ItemRequestDto> findAll(@RequestHeader("X-Sharer-User-Id") long seekerId,
                                        @RequestParam(defaultValue = "0") int from,
                                        @RequestParam(defaultValue = "20") int size) {
        Logging.logInfoIncomingRequest(log, "GET /requests/all", seekerId, from, size);
        return itemRequestService.findAll(seekerId, from, size);
    }

    @PostMapping
    public ItemRequestDto create(@RequestBody ItemRequestDto dto, @RequestHeader("X-Sharer-User-Id") long requesterId) {
        Logging.logInfoIncomingRequest(log, "POST /requests", dto, requesterId);
        dto.setRequesterId(requesterId);
        return itemRequestService.create(dto);
    }
}