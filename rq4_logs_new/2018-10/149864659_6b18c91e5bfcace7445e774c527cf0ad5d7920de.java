package com.website.jsonserializer;

import com.google.gson.*;
import com.website.entity.Product;

import java.lang.reflect.Type;

public class ProductSerializer implements JsonSerializer<Product> {
    @Override
    public JsonElement serialize(Product product, Type type, JsonSerializationContext jsonSerializationContext) {
        JsonObject result = new JsonObject();

        result.addProperty("title", product.getTitle());

        JsonArray reviews = new JsonArray();
        for (String review : product.getReview()) {
            reviews.add(jsonSerializationContext.serialize(review));
        }
        result.add("reviews", reviews);


        return result;
    }
}