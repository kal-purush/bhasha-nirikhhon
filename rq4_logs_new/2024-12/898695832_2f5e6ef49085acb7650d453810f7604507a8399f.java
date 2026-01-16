package ru.clevertec.carshowroom.dto.car;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.clevertec.carshowroom.dto.client.OutputClientShortDTO;
import ru.clevertec.carshowroom.dto.review.OutputReviewShortDTO;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OutputCarDTO {
    private Long id;
    private String model;
    private String brand;
    private int year;
    private int price;
    private Long categoryId;
    private String categoryName;
    private Long showroomId;
    private String showroomTitle;
    private List<OutputClientShortDTO> clients;
    private List<OutputReviewShortDTO> reviews;
}