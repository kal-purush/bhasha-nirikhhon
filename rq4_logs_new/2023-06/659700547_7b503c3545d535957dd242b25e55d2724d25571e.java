package com.example.backendeindopdracht.DTO.inputDto;

import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

public class producInputDto {

    @NotBlank
    private String name;
    @Min(value = 0 , message = "The product cant be negative in price")
    private double price;
    private int originalStock;
    private int currentStock;
    private String description;
}