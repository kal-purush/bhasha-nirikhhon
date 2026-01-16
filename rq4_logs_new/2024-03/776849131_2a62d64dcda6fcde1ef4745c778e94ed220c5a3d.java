import java.math.BigDecimal;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.stream.Stream;


public class Main {
    public static void main(String[] args) {
        var potatoProductName = "potato";
        var minPrice = BigDecimal.valueOf(9.99);
        var maxPrice = BigDecimal.valueOf(10);

        // Работа с датой - https://javarush.com/groups/posts/1941-kak-ne-poterjatjhsja-vo-vremeni--datetime-i-calendar
        var minDate = new GregorianCalendar(2040, Calendar.JANUARY, 1).getTime();
        var maxDate = new GregorianCalendar(2077, Calendar.JANUARY, 1).getTime();

        var products = Stream.of(
                        new Product(1, potatoProductName, "upc", "manufacturer", minPrice, minDate, 1) {
                        },
                        new Product(2, potatoProductName, "upc", "manufacturer", maxPrice, minDate, 1) {
                        },
                        new Product(2077, "not potato", "upc", "manufacturer", minPrice, maxDate, 1) {
                        }
                )
                .toList();
        var service = new ProductService(products);

        var productsByName = service.getProductsByName(potatoProductName);
        var productsByNameAndMinPrice = service.getProductsByNameWithLessOrEqualPrice(potatoProductName, minPrice);
        var productsByShelfDate = service.getProductsAfterShelfDate(minDate);

        System.out.println("Список товаров:\n" + products);
        System.out.println("Фильтрация по имени: (" + potatoProductName + ")\n" + productsByName.toString());
        System.out.println("Фильтрация по имени и максимальной цене: " + "(" + potatoProductName + ") " + "(" + minPrice + ")\n" + productsByNameAndMinPrice.toString());
        System.out.println("Фильтрация по сроку хранения больше заданного: (" + minDate + ")\n" + productsByShelfDate.toString());
    }
}