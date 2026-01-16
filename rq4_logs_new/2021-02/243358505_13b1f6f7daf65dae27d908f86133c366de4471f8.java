package mybase.domain;

import lombok.Data;
import mybase.domain.jpa.MainUser;

import javax.persistence.*;
import java.time.LocalDateTime;
import java.util.List;

@Data
@Entity
@Table(name = "goal_entities")
public class GoalEntity {

    @Id
    @GeneratedValue
    private Long goalID;

    private String goalName;

    private LocalDateTime goalTime;

    private Boolean isCompleted;

    @OneToMany
    private List<TaskEntity> taskEntityList;

    @ManyToOne(fetch = FetchType.EAGER)
    private MainUser mainUser;

}