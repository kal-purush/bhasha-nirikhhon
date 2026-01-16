package com.example.auth_service.dto;

import com.example.auth_service.model.ObjectType;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

/**
 * DTO для создания и обновления объекта недвижимости.
 */
@Data
public class ObjectRequestDto {

    @NotBlank(message = "Имя объекта не должно быть пустым")
    private String name;

    @NotNull(message = "Тип объекта не должен быть null")
    private ObjectType objectType;

    private Long parentId; // ID родителя (может быть null)
}