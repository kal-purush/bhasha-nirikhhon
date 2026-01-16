package eu.aequos.gogas.dto;

import eu.aequos.gogas.persistence.entity.Product;
import eu.aequos.gogas.persistence.entity.ProductCategory;
import lombok.Data;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

@Data
public class CategoryDTO {
    private String id;
    private String name;
    private String color;
    private List<SmallProductDTO> products;

    public static CategoryDTO fromModel(ProductCategory productCategory) {
        return fromModel(productCategory, null);
    }

    public static CategoryDTO fromModel(ProductCategory productCategory, List<Product> products) {
        CategoryDTO category = new CategoryDTO();
        category.id = productCategory.getId();
        category.name = productCategory.getDescription();
        category.color = productCategory.getPriceListColor();

        if (products != null) {
            category.products = products.stream()
                    .map(SmallProductDTO::fromModel)
                    .sorted(Comparator.comparing(SmallProductDTO::getName))
                    .collect(Collectors.toList());
        }

        return category;
    }

    @Data
    public static class SmallProductDTO {
        private String id;
        private String name;
        private String um;
        private String boxUm;
        private BigDecimal price;

        public static SmallProductDTO fromModel(Product product) {
            SmallProductDTO smallProduct = new SmallProductDTO();
            smallProduct.id = product.getId();
            smallProduct.name = product.getDescription();
            smallProduct.um = product.getUm();
            smallProduct.boxUm = product.getBoxUm();
            smallProduct.price = product.getPrice();
            return smallProduct;
        }
    }
}