package com.toyhe.app.Products.Modal;

import com.toyhe.app.Flotte.Models.BoatClass;
import com.toyhe.app.Price.Model.PriceModel;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@NoArgsConstructor
@AllArgsConstructor
@Data
@Builder
public class Products {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long productId;
    private String productName;
    private String productDescription;
    private String productType ;
    int quantity;
    @ManyToOne
    @JoinColumn(name = "price_id", referencedColumnName = "priceID")
    private PriceModel productPriceModel;
    @ManyToOne
    private ProductCategorization productCategorization;
    @ManyToOne
    private BoatClass boatClass;
}