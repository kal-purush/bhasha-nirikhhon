package lesson_17_Stream_API.homework;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MainProductCategoryAnalyze {
    public static void main(String[] args) {
        List<Product> products = Arrays.asList(
                new Product("Laptop", "Electronics", 1200.0),
                new Product("Coffee Maker", "Appliances", 80.0),
                new Product("Headphones", "Electronics", 150.0),
                new Product("Blender", "Appliances", 50.0)
        );

        System.out.println("\nЗгрупуйте продукти за їхніми категоріями:");
        Map<String, List<Product>> groupedByCategory = products.stream()
                .collect(Collectors.groupingBy(Product::getCategory));
        groupedByCategory.forEach((category, list) ->
                System.out.println(category + ": " + list));

        System.out.println("\nЗнайдіть середню ціну продуктів в кожній категорії:");
        Map<String, Double> avgPriceByCategory = products.stream()
                .collect(Collectors.groupingBy(Product::getCategory,
                        Collectors.averagingDouble(Product::getPrice)));
        System.out.println(avgPriceByCategory);

        System.out.println("\nЗнайдіть категорію з найвищою середньою ціною та виведіть її в консоль:");
        Map.Entry<String,Double> mostExpensiveCategory =
                avgPriceByCategory.entrySet().stream()
                        .sorted((e1, e2) ->
                                Double.compare(e2.getValue(), e1.getValue()))
                        .findFirst().get();

        System.out.println(
                mostExpensiveCategory.getKey() + "="
                        + mostExpensiveCategory.getValue());
    }
}

/*
CONSOLE OUTPUT:
Згрупуйте продукти за їхніми категоріями:
Appliances: [Product{name='Coffee Maker', category='Appliances', price=80.0},
             Product{name='Blender', category='Appliances', price=50.0}]
Electronics: [Product{name='Laptop', category='Electronics', price=1200.0},
              Product{name='Headphones', category='Electronics', price=150.0}]
Знайдіть середню ціну продуктів в кожній категорії:
{Appliances=65.0, Electronics=675.0}
Знайдіть категорію з найвищою середньою ціною та виведіть її в консоль:
Electronics=675.0
 */