package ru.nsu.peretyatko.dto.militaries;

import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class MilitaryPropertyPatchRequest {
    private Integer militaryId;

    @Size(min = 2, max = 64, message = "Title must be between 2 and 64 characters long.")
    private String title;

    private Integer rankId;

    @NotEmpty(message = "Value cannot be empty.")
    private String value;
}