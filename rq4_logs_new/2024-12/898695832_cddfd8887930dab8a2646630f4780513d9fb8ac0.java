package ru.clevertec.carshowroom.dto.review;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.clevertec.carshowroom.dto.car.OutputCarShortDTO;
import ru.clevertec.carshowroom.dto.client.OutputClientShortDTO;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OutputReviewDTO {
    private Long id;
    private String text;
    private int rating;
    private OutputCarShortDTO car;
    private OutputClientShortDTO client;
}