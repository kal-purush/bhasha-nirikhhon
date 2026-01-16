package TT.tripTogether.domain.route;

import TT.tripTogether.domain.attraction.Attraction;
import lombok.Getter;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.util.ArrayList;
import java.util.List;

@Entity
@Getter
@NoArgsConstructor
public class Route {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "route_id")
    private Long id;

    @OneToMany(mappedBy = "route", cascade = CascadeType.ALL)
    private List<Attraction> attractions = new ArrayList<>();
}