package dat3.adventureXP.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.OneToMany;
import lombok.*;

import java.util.List;

@Getter
@Setter
@AllArgsConstructor
@Builder //We will talk about this in the class
@NoArgsConstructor
// ----Lombok anotations above --------- //
@Entity
public class Activity {
    /*Name (PK) [str]
Status (enums)
AgeLimit [int]
Description [String]
Reservation [List]
Equipment [List]
User (employee) [Class]
*/
    @Id
    @Column(name="activity_name", length = 30, nullable = false, unique = true)
    private String name;

    @Column(name="activity_status", length = 10, nullable = false)
    private String status;

    @Column(name="activity_agelimit", nullable = false)
    private int ageLimit;

    @Column(name="activity_description", length = 100, nullable = false, unique = true)
    private String description;

    @OneToMany(mappedBy = "activities")
    private List<Reservation> reservations;

    @Column(name="", length = , nullable = false, unique = true)
    private User employee;


}