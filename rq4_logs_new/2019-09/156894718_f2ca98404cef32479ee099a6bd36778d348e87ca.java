package com.bin448.backend.controller;


import com.bin448.backend.entity.DTOentity.NewRoomPriceDTO;
import com.bin448.backend.service.NewRoomPriceService;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping("/newRoomPrice")
public class NewRoomPriceController {

    //dodaj u authent...
    private final NewRoomPriceService newRoomPriceService;

    public NewRoomPriceController(NewRoomPriceService newRoomPriceService) {
        this.newRoomPriceService = newRoomPriceService;
    }

    @PostMapping("/checkExistence")
    public ResponseEntity<Boolean> checkExistence(@RequestBody NewRoomPriceDTO newRoomPriceDTO) {
        return ResponseEntity.ok(newRoomPriceService.checkIfPricesAlreadyExist(newRoomPriceDTO));
    }


    @PostMapping
    public ResponseEntity<String> addHotel(@RequestBody NewRoomPriceDTO newRoomPriceDTO) {
        newRoomPriceService.addNewPrice(newRoomPriceDTO);
        return ResponseEntity.ok("New price is added");
    }
}