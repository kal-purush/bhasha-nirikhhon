package com.example.backendeindopdracht.controller;


import com.example.backendeindopdracht.DTO.inputDto.OrderInputDTO;
import com.example.backendeindopdracht.DTO.inputDto.ProductInputDTO;
import com.example.backendeindopdracht.DTO.outputDto.OrderOutputDTO;
import com.example.backendeindopdracht.DTO.outputDto.ProductOutputDTO;
import com.example.backendeindopdracht.model.Order;
import com.example.backendeindopdracht.service.OrderService;
import jakarta.validation.Valid;
import lombok.AllArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.BindingResult;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@AllArgsConstructor
@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderService orderService;

    //POST
    @PostMapping("/add")
    public ResponseEntity<OrderOutputDTO> addOrder (@RequestBody OrderInputDTO orderInputDTO){
        OrderOutputDTO addedOrder = orderService.addOrder(orderInputDTO);
        return ResponseEntity.status(HttpStatus.CREATED).body(addedOrder);
    }





}