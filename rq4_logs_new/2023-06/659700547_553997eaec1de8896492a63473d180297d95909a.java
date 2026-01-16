package com.example.backendeindopdracht.controller;

import com.example.backendeindopdracht.service.RepairService;
import lombok.AllArgsConstructor;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@AllArgsConstructor
@RestController
@RequestMapping("/repairs")
public class RepairController {

    private final RepairService repairService;
}