package com.example.blogs.dtos;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class PostRequestDto
{
    @NotEmpty(message = "Post Name cannot be empty")
    private String postName;
    private String content;
    @NotNull(message = "category Id cannot empty")
    private Integer categoryId;
}