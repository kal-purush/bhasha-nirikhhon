package ru.clevertec.carshowroom.dto.car;

import com.fasterxml.jackson.annotation.JsonFormat;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PastOrPresent;
import jakarta.validation.constraints.Positive;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDate;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class CarRequest {
    @NotBlank(message = "Model should not be blank.")
    private String model;

    @NotBlank(message = "Brand should not be blank.")
    private String brand;

    @NotNull(message = "Production date should be provided.")
    @PastOrPresent(message = "Production date should be in the past or today.")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
    private LocalDate productionDate;

    @NotNull(message = "Price should not be null.")
    @DecimalMin(message = "Price should be positive.",
            value = "0", inclusive = false)
    private BigDecimal price;

    @NotNull(message = "Category ID should not be null.")
    @Positive(message = "Category ID should be a positive number.")
    private Long categoryId;
}