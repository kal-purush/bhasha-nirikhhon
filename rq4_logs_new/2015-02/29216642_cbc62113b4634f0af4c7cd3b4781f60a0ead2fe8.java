package am.te.myapplication;

/**
 * The product class represents a product that a user desires.
 *
 * @author Mitchell Manguno
 * @since 2015 February 23
 */

public class Product {

    public String name;
    public double desiredPrice;
    public String additionalInfo;

    public Product(String name, double desiredPrice, String additionalInfo) {
        this.name = name;
        this.desiredPrice = desiredPrice;
        this.additionalInfo = additionalInfo;
    }

    public Product(String name, double desiredPrice) {
        this(name, desiredPrice, null);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getDesiredPrice() {
        return desiredPrice;
    }

    public void setDesiredPrice(double desiredPrice) {
        this.desiredPrice = desiredPrice;
    }

    public String getAdditionalInfo() {
        return additionalInfo;
    }

    public void setAdditionalInfo(String additionalInfo) {
        this.additionalInfo = additionalInfo;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }

        if (!(other instanceof Product)) {
            return false;
        }

        Product o = (Product) other;

        return o.name.equals(o.name) && o.desiredPrice == o.desiredPrice
            && o.additionalInfo.equals(o.additionalInfo);
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash += 31 * hash + name.hashCode();
        hash += 127 * hash + desiredPrice;
        hash += 8191 * hash + additionalInfo.hashCode();
        return hash;
    }

    @Override
    public String toString() {
        return "Name: " + name + ", Price: " + desiredPrice
            + ", Additional Info: " + additionalInfo;
    }

 }