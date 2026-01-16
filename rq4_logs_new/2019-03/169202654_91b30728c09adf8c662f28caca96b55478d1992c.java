package com.gcp.cart.model;

import java.util.List;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

/**
 * @author Anuj Kumar
 * 
 * This class represent the product details
 */
@Data
public class Product {
	private String id;
    private String productName;
    private String shortDescription;
    private String longDescription;
    private String canonicalUrl;
    private String brand;
    private String skuId;
    private String currencyCode;
    private String weight;
    private String length;
    private String width;
    private String height;
    private List<String> categories;
    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
    private String category1;
    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
    private String category2;
    private int stockQuantity;
    private double price;
    private List<String> images;
}