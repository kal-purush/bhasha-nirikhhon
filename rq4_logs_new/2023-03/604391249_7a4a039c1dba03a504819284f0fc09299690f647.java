package com.abneco.delivery.purchase.entity;

import com.abneco.delivery.product.entity.Product;
import com.abneco.delivery.user.entity.Buyer;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.GenericGenerator;

import javax.persistence.*;
import javax.validation.constraints.NotNull;
import java.math.BigDecimal;

@Entity
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class Purchase {

    @Id
    @GeneratedValue(generator = "uuid")
    @GenericGenerator(name = "uuid", strategy = "uuid2")
    @Column(name = "purchase_id")
    private String id;

    @ManyToOne
    private Buyer buyer;

    @ManyToOne
    @NotNull
    private Product product;

    @NotNull
    private int quantity;

    @NotNull
    private BigDecimal finalPrice;

    @Column(name = "purchasedAt")
    private String purchasedAt;
}