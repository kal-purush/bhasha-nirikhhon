package org.acme.foodpackaging.domain;

import java.util.Map;

public enum GlazeType {
    C6,
    C4,
    G15,
    C65_47,
    ALENKA,
    CARAMEL,
    CACTUS;

    private static final Map<ProductType, GlazeType> DEFAULT_BY_TYPE = Map.of(
            ProductType.CLASSIC, C4,
            ProductType.ROD, C6,
            ProductType.PLUSH, C65_47,
            ProductType.CACTUS, CACTUS
    );

    private static final Map<String, GlazeType> ID_TO_GLAZE = Map.of(
            "4810268043710", ALENKA,
            "4810268043475", C65_47,
            "4810268050282", C65_47,
            "4810268040450", CARAMEL,
            "4810268043727", G15
    );

    public static GlazeType getDefaultForType(ProductType type) {
        return DEFAULT_BY_TYPE.getOrDefault(type, C4);
    }

    public static GlazeType fromProduct(String productId, ProductType type) {
        // 1. Попробовать получить по ID продукта
        GlazeType glaze = ID_TO_GLAZE.get(productId);
        if (glaze != null) {
            return glaze;
        }

        // 2. Вернуть дефолт для типа
        return getDefaultForType(type);
    }
}