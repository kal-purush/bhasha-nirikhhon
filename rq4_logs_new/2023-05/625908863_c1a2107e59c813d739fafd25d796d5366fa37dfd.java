package com.brainstrom.meokjang.food.dto.request;

import com.brainstrom.meokjang.food.domain.Food;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;

import java.time.LocalDate;

@Getter
@NoArgsConstructor
public class FoodDto {
    private String foodName;
    private Integer stock;
    @DateTimeFormat(pattern = "yyyy-MM-dd")
    private LocalDate expireDate;
    private String storageWay;

    public Food toEntity(){
        Food food = new Food();
        food.setFoodName(this.foodName);
        food.setStock(this.stock);
        food.setExpireDate(this.expireDate);
        food.setStorageWay(this.storageWay);

        return food;
    }

    public FoodDto(String foodName, Integer stock, String expireDate, String storageWay) {
        this.foodName = foodName;
        this.stock = stock;
        this.expireDate = LocalDate.parse(expireDate);
        this.storageWay = storageWay;
    }
}