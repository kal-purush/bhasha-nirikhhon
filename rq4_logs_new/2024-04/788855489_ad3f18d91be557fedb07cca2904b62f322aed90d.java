package growthook.org.bamgang.members.domain;

import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

import java.util.Date;

@Entity
@Getter
@Setter
@Table(name = "pickedtrail")
public class PickedWalk {
    @JsonInclude(JsonInclude.Include.NON_NULL)

    @Id
    private int trailId;

    private int userId;

    private Date pickedDay;

    private String trailRegion;


}