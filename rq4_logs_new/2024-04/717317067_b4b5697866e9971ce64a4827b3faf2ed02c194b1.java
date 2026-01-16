package sit.cp23ms2.sportconnect.dtos.location;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class CreateLocationDto {
    private String name;
    private Double latitude;
    private Double longitude;
}