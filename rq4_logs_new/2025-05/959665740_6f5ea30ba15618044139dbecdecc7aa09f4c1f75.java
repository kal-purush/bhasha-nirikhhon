package com.example.backend.dtos.subDTO;

import com.example.backend.dtos.ResidentDTO;
import com.example.backend.models.Apartment;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.stream.Collectors;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ApartmentDetailDTO {
    private Long id;
    private String name;
    private Integer area;

    @JsonIgnoreProperties("apartment")
    private List<ResidentDTO> residents;


    public static ApartmentDetailDTO fromEntity(Apartment apartment) {
        List<ResidentDTO> residentDTOs = apartment.getResidents() != null
                ? apartment.getResidents().stream().map(ResidentDTO::fromEntity).collect(Collectors.toList())
                : List.of();

        return new ApartmentDetailDTO(
                apartment.getId(),
                apartment.getName(),
                apartment.getArea(),
                residentDTOs
        );
    }
}