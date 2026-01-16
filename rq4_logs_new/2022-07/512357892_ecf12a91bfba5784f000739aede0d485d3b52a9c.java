package TT.tripTogether.domain.attraction.dto;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.data.geo.Point;

@Getter
@Setter
@NoArgsConstructor
public class AttractionDto {
    private String name;

    //위도, 경도
    private Point point;
}