package com.example.demo.furnitureapi.api;

import com.example.demo.furnitureapi.dto.FurnitureDTO;
import com.example.demo.furnitureapi.service.FurnitureService;
import com.example.demo.productapi.dto.ProductDTO;
import com.example.demo.productapi.service.ProductService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@Slf4j
@RequiredArgsConstructor
@RequestMapping("/furniture")
@CrossOrigin
public class FurnitureController {

    private final FurnitureService furnitureService;

    @GetMapping(path = "/{pageNum}", produces = "application/json; charset=UTF-8")
    public ResponseEntity<List<FurnitureDTO>> getFurnitures(@PathVariable int pageNum) {
        List<FurnitureDTO> furnitures = furnitureService.getFurnitures(pageNum);

        return new ResponseEntity<>(furnitures, HttpStatus.OK);
    }

}