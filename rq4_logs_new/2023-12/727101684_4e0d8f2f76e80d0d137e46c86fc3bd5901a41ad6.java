package com.example.demo.recipeapi.entity;

import com.example.demo.userapi.entity.User;
import lombok.*;

import javax.persistence.*;

@Getter @Setter
@ToString
@EqualsAndHashCode(of = "id")
@NoArgsConstructor
@AllArgsConstructor
@Builder

@Entity
@Table(name = "tbl_like")
public class Like {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "like_id")
    private int id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "user_id")
    private User user;

    //    @ManyToOne(fetch = FetchType.LAZY)
    @Column(name = "recipe_name")
    private String recipeName; // 프론트에서 받아서 넣기

}