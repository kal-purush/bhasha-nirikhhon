package buta.creditapplication.Entity;

import jakarta.persistence.*;
import lombok.*;
import lombok.experimental.FieldDefaults;

@Entity
@Table(name = "credit")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class Credit {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    Integer id;
    int amount;
    int interest;
    int year;
    int monthly;
    int total;
    int commission;
}