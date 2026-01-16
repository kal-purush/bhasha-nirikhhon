import java.util.Locale;

public abstract class AbstractPurchase implements Comparable<AbstractPurchase> {
    private final Product product;
    private int amount;
    protected double cost;

    public AbstractPurchase(Product product, int amount) {
        this.product = product;
        this.amount = amount;
    }

    public int getCost() {
        return product.getPrice() * amount / 100;
    }

    // "each NON CONSTANT field" - Product is constant?
    @Override
    public String toString() {
        String formattedCost = String.format(Locale.ROOT, "%.02f", cost);
        return amount + ";" + formattedCost;
    }

    @Override
    public int compareTo(AbstractPurchase o) {
        if (this.getCost() < o.getCost())
            return 1;
        else if (this.getCost() == o.getCost())
            return 0;
        else
            return -1;
    }

    public Product getProduct() {
        return product;
    }

    public int getAmount() {
        return amount;
    }
}