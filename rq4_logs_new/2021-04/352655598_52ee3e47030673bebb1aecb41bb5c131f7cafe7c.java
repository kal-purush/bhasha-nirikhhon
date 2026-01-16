package com.kodilla.ecommerce.domain;

import com.sun.istack.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import javax.validation.constraints.DecimalMin;
import javax.validation.constraints.Size;
import java.math.BigDecimal;

@NoArgsConstructor
@AllArgsConstructor
@Builder
@Data
@Entity
@Table(name = "products")
public class Product {

    @NotNull
    @Id
    @GeneratedValue
    private Long id;
    
    @Size(min = 4, max = 50)
    private String name;

    @Size(max = 1000)
    private String description;

    @DecimalMin(value = "1.0")
    private BigDecimal price;

    @Column(name = "group_id")
    private Long groupId;

}
