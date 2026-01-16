package vn.hust.hedspi.ezsport.dtos.field;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;
import vn.hust.hedspi.ezsport.entities.Sport;
import vn.hust.hedspi.ezsport.entities.User;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE)
public class FieldResponse {
    String id;
    String name;
    User owner;
    String isActive;
    String description;
    String status;
    double latitude;
    double longitude;
    String block;
    String avatar;
    Sport sport;
}