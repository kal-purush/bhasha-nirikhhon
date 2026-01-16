package upeldev.com.github.upel3.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.Entity;
import javax.persistence.ManyToOne;

@Getter
@Setter
@NoArgsConstructor
@Entity
public class GroupGrade extends Grade{

    @JsonIgnore
    @ManyToOne
    private StudentGroup group;

    public GroupGrade(StudentGroup group, Activity activity) {
        super(activity);
        this.group = group;
    }
}