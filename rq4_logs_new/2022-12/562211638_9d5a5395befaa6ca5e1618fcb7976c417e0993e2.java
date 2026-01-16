package ru.vasili_zlobin.springdatajpa.specifications;

import org.springframework.data.jpa.domain.Specification;
import ru.vasili_zlobin.springdatajpa.model.Product;

public class ProductSpecification {
    public static Specification<Product> priceGreaterThan(Double min) {
        return (root, query, criteriaBuilder) -> criteriaBuilder.greaterThan(root.get("price"), min);
    }

    public static Specification<Product> priceLessThan(Double max) {
        return (root, query, criteriaBuilder) -> criteriaBuilder.lessThan(root.get("price"), max);
    }
}