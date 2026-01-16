package dat3.adventureXP.entity;

import dat3.security.entity.UserWithRoles;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.ArrayList;

@Getter
@Setter
@NoArgsConstructor
// ----Lombok anotations above --------- //
@Entity
@Inheritance(strategy = InheritanceType.SINGLE_TABLE)
@DiscriminatorColumn(name = "USER_TYPE")
public class User extends UserWithRoles {
    String name;
    String role;
    int age;
    String email;
    String password;

    public User(String user, String password, String email, String name, String role, int age) {
        super(user, password, email);
        this.name = name;
        this.role = role;
        this.age = age;
    }
   /* Implement when reservations are implemented
    @OneToMany(mappedBy = "user")

    List<Reservation> reservations;

    public void addReservation(Reservation reservation){
        if (reservations == null){
            reservations = new ArrayList<>();
        }
        reservations.add(reservation);
    } */
}