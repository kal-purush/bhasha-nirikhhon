import java.util.Objects;

public class Product {

    private String name;
    private double productCoast;
    private double weightInKilograms;

    public Product(String name, double productCoast, double weightInKilograms) {
        this.name = name;
        this.productCoast = productCoast;
        this.weightInKilograms = weightInKilograms;

        if (name == null || name.isBlank() || name.isEmpty()) // проверка если строка не пустая, если нет пробела, отсутствует значение
        {
            throw new RuntimeException("Введите имя продукта");
        }
        if (productCoast < 0) {
            throw new RuntimeException("Цена продукта отсутствует");
        }
        if (weightInKilograms < 0) {
            throw new RuntimeException("Вес продукта отсутствует");
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getProductCoast() {
        return productCoast;
    }

    public void setProductCoast(double productCoast) {
        this.productCoast = productCoast;
    }

    public double getWeightInKilograms() {
        return weightInKilograms;
    }

    public void setWeightInKilograms(double weightInKilograms) {
        this.weightInKilograms = weightInKilograms;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Product)) return false;
        Product product = (Product) o;
        return Double.compare(product.getProductCoast(), getProductCoast()) == 0 && Double.compare(product.getWeightInKilograms(), getWeightInKilograms()) == 0 && getName().equals(product.getName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName(), getProductCoast(), getWeightInKilograms());
    }

    @Override
    public String toString() {
        return "Product{" +
                "name='" + name + '\'' +
                ", productCoast=" + productCoast +
                ", weightInKilograms=" + weightInKilograms +
                '}';
    }
}