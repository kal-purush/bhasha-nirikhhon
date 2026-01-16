package com.example.pizza.model;

import com.example.pizza.entity.Product;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ProductDTO {
    private Integer id;
    private String name;
    private String description;
    private String image;
    private Boolean spicy;
    private String type;
    private String category;
    private String pricesJson; // Trường giá trị JSON của prices

    public ProductDTO(Product product) throws JsonProcessingException {

        this.id = product.getId();
        System.out.println(product.getName());
        this.name = product.getName();
        this.description = product.getDescription();
        this.image = product.getImage();
        this.spicy = product.getSpicy();
        this.type = product.getType().name();
        this.category = product.getCategory().name();

        // Chuyển prices thành JSON
        ObjectMapper objectMapper = new ObjectMapper();
        this.pricesJson = objectMapper.writeValueAsString(product.getPrices());
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    public Boolean getSpicy() {
        return spicy;
    }

    public void setSpicy(Boolean spicy) {
        this.spicy = spicy;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getPricesJson() {
        return pricesJson;
    }

    public void setPricesJson(String pricesJson) {
        this.pricesJson = pricesJson;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

}