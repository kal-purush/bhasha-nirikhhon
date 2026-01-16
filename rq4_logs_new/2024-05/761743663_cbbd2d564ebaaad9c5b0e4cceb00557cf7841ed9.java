package com.buildnow.springbootapp.buildnowspringboot.dto;

import com.buildnow.springbootapp.buildnowspringboot.ENUM.UpperCategoryENUM;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class GradingRecruiterDTO {
    private String category;
    private UpperCategoryENUM upperCategoryENUM;

    @Builder
    public GradingRecruiterDTO(
            String category,
            UpperCategoryENUM upperCategoryENUM
    ){
        this.category = category;
        this.upperCategoryENUM = upperCategoryENUM;
    }
}