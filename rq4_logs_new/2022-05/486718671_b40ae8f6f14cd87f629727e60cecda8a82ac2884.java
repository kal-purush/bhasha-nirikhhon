package com.epam.nazar.grinko.domians;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.persistence.*;
import java.util.Collection;

@Getter
@Setter
@Entity
@Table(name = "car_color")
@Accessors(chain = true)
public class CarColor {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long id;

    @Column(name = "color", nullable = false, unique = true)
    private String color;

    @OneToMany(mappedBy = "color", fetch = FetchType.EAGER)
    private Collection<Car> cars;
}