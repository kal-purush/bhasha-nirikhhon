package com.example.demo.db.entity;

import jakarta.persistence.*;
import jakarta.validation.constraints.Size;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Set;

/**
 * @author Sattya
 * create at 1/28/2024 3:21 PM
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "brands",indexes = {
        @Index(name = "idx_brand_name",columnList = "brand_name",unique = true)
})
public class BrandEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;
    @Column(unique = true,nullable = false)
    private String uuid;
    @Size(max = 100)
    @Column(name = "brand_name",nullable = false)
    private String name;

    @ManyToMany(mappedBy = "brands")
    private Set<CategoryEntity> categories;

    @OneToMany(mappedBy = "brand")
    private Set<ProductEntity> products;
}