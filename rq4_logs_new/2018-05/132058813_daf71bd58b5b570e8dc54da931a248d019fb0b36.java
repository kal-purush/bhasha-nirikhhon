package by.ras.entity.particular;

import by.ras.entity.BaseEntity;
import lombok.*;

import javax.persistence.*;
import javax.validation.constraints.*;

@Getter @Setter @ToString(exclude = "user")
@NoArgsConstructor @AllArgsConstructor @Builder
@Inheritance(strategy = InheritanceType.JOINED)
@Table(name = "contacts")
@Entity
public class Contact extends BaseEntity{

    @OneToOne
    @JoinColumn(name = "user_id", nullable = false, unique = true)
    private User user;

    @Column
    @Pattern(regexp = "^[A-Z]+[a-z]+$")
    @Max(value = 15, message = "Max country length should be less then 15 characters")
    @Min(value = 1, message = "Min country length should be great then 1 characters")
    private String country;
    @Column
    @Pattern(regexp = "^[A-Z]+[a-z]+$")
    @Max(value = 15, message = "Max city length should be less then 15 characters")
    @Min(value = 1, message = "Min city length should be great then 1 characters")
    private String city;
    @Column
    @Pattern(regexp = "^[A-Z]+[a-z]+$")
    @Max(value = 15, message = "Max street length should be less then 15 characters")
    @Min(value = 1, message = "Min street length should be great then 1 characters")
    private String street;
    @Column
    @DecimalMax(value = "200", message = "Digit must be a less than 200")
    @DecimalMin(value = "0", message = "Digit must be a greater than 0")
    private int houseNumder;
    @Column
    //any 11 numbers
    @Pattern(regexp = "^[0-9]{11,11}$")
    private int phoneNumber;



}