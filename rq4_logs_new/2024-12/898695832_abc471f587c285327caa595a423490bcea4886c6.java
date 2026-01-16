package ru.clevertec.carshowroom.dto.client;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.clevertec.carshowroom.dto.car.OutputCarShortDTO;
import ru.clevertec.carshowroom.dto.review.OutputReviewShortDTO;

import java.util.Date;
import java.util.List;
import java.util.Set;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OutputClientDTO {
    private Long id;
    private String name;
    private Set<String> contacts;
    private Date dateOfRegistration;
    private List<OutputCarShortDTO> cars;
    private List<OutputReviewShortDTO> reviews;
}