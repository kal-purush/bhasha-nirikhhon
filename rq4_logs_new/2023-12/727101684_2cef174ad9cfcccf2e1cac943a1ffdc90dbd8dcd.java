package com.example.demo.recipeapi.dto;

import lombok.*;

import javax.validation.constraints.NotBlank;

@Getter @Setter
@ToString
@EqualsAndHashCode
@NoArgsConstructor
@Builder
public class LikeRequestDTO {

    @NotBlank
    private String userId;
    @NotBlank
    private String recipeName;
    @Builder.Default
    private boolean done = false;

    public LikeRequestDTO(String userId, String recipeName, boolean done) {
        this.userId = userId;
        this.recipeName = recipeName;
        this.done = done;
    }

}