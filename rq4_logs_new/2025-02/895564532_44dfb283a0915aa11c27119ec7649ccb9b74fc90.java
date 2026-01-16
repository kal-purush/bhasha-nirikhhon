package com.app.chat.controller;

import com.app.chat.service.RoomService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("api/v1/room")
public class RoomController {

    private final RoomService roomService;

    public RoomController(RoomService roomService) {
        this.roomService = roomService;
    }

    @PostMapping("/create")
    public ResponseEntity<?> createRoom(@RequestParam String roomId) {
        return new ResponseEntity<>(roomService.createRoom(roomId), HttpStatus.CREATED);
    }

    @GetMapping("/join/{roomId}")
    public ResponseEntity<?> joinRoom(@PathVariable String roomId) {
        return new ResponseEntity<>(roomService.joinRoom(roomId), HttpStatus.OK);
    }
}