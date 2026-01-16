package com.example.backend.dtos.subDTO;

import com.example.backend.models.Apartment;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonPropertyOrder({
        "id",
        "name",
        "area",
        "residentCount",
        "date_created"
})
public class ApartmentSummaryDTO {
    private Long id;
    private String name;
    private Integer area;
    private Integer residentCount;

    @JsonProperty("date_created")
    private LocalDate dateCreated;

    public static ApartmentSummaryDTO fromEntity(Apartment apartment) {
        return new ApartmentSummaryDTO(
                apartment.getId(),
                apartment.getName(),
                apartment.getArea(),
                apartment.getResidents() != null ? apartment.getResidents().size() : 0,
                apartment.getCreatedAt().toLocalDate()
        );
    }
}