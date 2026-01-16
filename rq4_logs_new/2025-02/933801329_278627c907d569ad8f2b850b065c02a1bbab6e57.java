package com.apiautomation.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ResponseItem {

    /*
     * digunakan untuk maintenance response" pathnya
     */

    /*
     * {
     * "id": 195,
     * "title": "Essence Mascara Lash Princess",
     * "price": 9.99,
     * "discountPercentage": 7.17,
     * "stock": 5,
     * "rating": 4.94,
     * "description":
     * "The Essence Mascara Lash Princess is a popular mascara known for its volumizing and lengthening effects. Achieve dramatic lashes with this long-lasting and cruelty-free formula."
     * ,
     * "category": "beauty"
     * }
     */

    @JsonProperty("id")

    public String id;

    @JsonProperty("title")
    public String title;

    @JsonProperty("price")
    public int price;

    @JsonProperty("discountPercentage")
    public int discountPercentage;

    @JsonProperty("stock")
    public int stock;

    @JsonProperty("rating")
    public int rating;

    @JsonProperty("description")
    public String description;

    @JsonProperty("category")
    public String category;

}