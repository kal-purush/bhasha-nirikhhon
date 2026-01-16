package pl.quayal.app.server.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;

import javax.persistence.*;
import java.util.Set;

@Entity
@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class Facilitator extends Trainer{

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @OneToMany(mappedBy = "facilitator", cascade = CascadeType.ALL)
    private Set<Course> courses;

    public Facilitator(String firstName, String lastName) {
        super(firstName, lastName);
    }
}
