package com.example.demo.mealkitapi.api;

import com.example.demo.mealkitapi.dto.MealKitDTO;
import com.example.demo.mealkitapi.service.MealKitService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@Slf4j
@RequiredArgsConstructor
@RequestMapping("/mealKit")
@CrossOrigin
public class MealKitController {

    private final MealKitService mealKitService;

    @GetMapping(path = "/{pageNum}", produces = "application/json; charset=UTF-8")
    public ResponseEntity<List<MealKitDTO>> getMealKits(@PathVariable int pageNum) {
        List<MealKitDTO> mealKits = mealKitService.getMealKits(pageNum);

        return new ResponseEntity<>(mealKits, HttpStatus.OK);
    }

}