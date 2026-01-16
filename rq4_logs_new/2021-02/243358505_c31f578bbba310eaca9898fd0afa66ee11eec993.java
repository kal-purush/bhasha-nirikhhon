package mybase.domain.jpa;


import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.util.UUID;

@Data
@Entity
@Table(name = "goal_key_points")
@NoArgsConstructor
public class GoalKeypoint {

    @Id
    @Column(name = "keypoint_uuid")
    private UUID keypointUUID;

    private String keypointName, keypointDescription;

    public GoalKeypoint(String keypointName, String keypointDescription) {
        this.keypointUUID = UUID.randomUUID();
        this.keypointName = keypointName;
        this.keypointDescription = keypointDescription;
    }

}