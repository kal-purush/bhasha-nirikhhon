package ua.ithillel.lesson30;

import jakarta.persistence.*;
import lombok.Data;

@Data

@Entity
@Table(name="heroinfo")
public class HibernateHero {
    @Id
    @Column(name="id")
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int id;

    public HibernateHero() {}

    // --- конструктор без id, так как id генерируется автоматически в БД
    public HibernateHero(String name, String gender, String eyeColor, String race, String hairColor, double height,
                         String publisher, String skinColor, String alignment, int weight) {
        this.name = name;
        this.gender = gender;
        this.eyeColor = eyeColor;
        this.race = race;
        this.hairColor = hairColor;
        this.height = height;
        this.publisher = publisher;
        this.skinColor = skinColor;
        this.alignment = alignment;
        this.weight = weight;
    }

    String name;
    String gender;
    @Column(name = "eye_color")
    String eyeColor;
    String race;
    @Column(name = "hair_color")
    String hairColor;
    double height;
    String publisher;
    @Column(name = "skin_color")
    String skinColor;
    String alignment;
    int weight;
}