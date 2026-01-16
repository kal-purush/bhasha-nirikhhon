package com.cibertec.proyecto.integrador.controller;

import com.cibertec.proyecto.integrador.entity.RoomEntity;
import com.cibertec.proyecto.integrador.services.RoomService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/rooms")
public class RoomController {
    @Autowired
    private RoomService roomService;

    @GetMapping("")
    public List<RoomEntity>listRooms() {
        return roomService.listarRooms();
    }

    @GetMapping("/{id}")
    public RoomEntity getRoomtoId(@PathVariable int id) {
        return roomService.getRoomToId(id);
    }

    @GetMapping("/nameroom/{name}")
    public List<RoomEntity> getRoomToName(@PathVariable String name) {
        return roomService.getRoomToName(name);
    }

    @GetMapping("/floorhotel/{floor}")
    public List<RoomEntity> getRoomToFloor(@PathVariable int floor) {
        return roomService.getRoomToFloor(floor);
    }
}