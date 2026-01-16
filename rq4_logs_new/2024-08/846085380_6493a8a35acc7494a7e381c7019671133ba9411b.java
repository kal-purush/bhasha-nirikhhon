package vn.hust.hedspi.ezsport.dtos.fieldOrder;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;
import vn.hust.hedspi.ezsport.entities.Field;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE)
public class FieldOrderResponse {
    String id;
    Field field;
    LocalTime start;
    LocalTime end;
    LocalDate date;
    double price;
    String userId;
    LocalDateTime paidAt;
}