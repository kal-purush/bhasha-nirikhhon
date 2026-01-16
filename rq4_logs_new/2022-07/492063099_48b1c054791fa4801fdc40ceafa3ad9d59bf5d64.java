package ma.nttsquad.nttecomcore.model;

import lombok.*;

import javax.persistence.*;
import java.util.ArrayList;
import java.util.List;

@Entity
@Table(name = "Currencies")
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
public class Currency {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "currency_id")
    private Long id;

    @Column(name = "currency_libelle", length = 15, nullable = false)
    private String currency;

    @OneToMany(mappedBy = "currency")
    @ToString.Exclude
    List<Order> orderList = new ArrayList<>();

    @OneToMany(mappedBy = "currency")
    @ToString.Exclude
    List<User> userList = new ArrayList<>();

    @OneToMany(mappedBy = "currency")
    @ToString.Exclude
    List<Product> productList = new ArrayList<>();
}