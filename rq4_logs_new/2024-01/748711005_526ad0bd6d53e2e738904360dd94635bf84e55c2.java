package com.example.demo.db.entity;

import jakarta.persistence.*;
import lombok.*;

import java.util.List;

/**
 * @author Sattya
 * create at 1/27/2024 1:38 PM
 */
@Entity
@Table(name = "categories",indexes = {
        @Index(name = "idx_category_name",columnList = "name",unique = true)
})
@Getter
@Setter
public class CategoryEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id",nullable = false)
    private Integer id;

    @Column(unique = true,nullable = false)
    private String uuid;

    @Column(name = "category_name",nullable = false)
    private String name;

    @Column(columnDefinition = "TEXT")
    private String description;

    @OneToMany(mappedBy = "category")
    private List<ProductEntity> products;

}