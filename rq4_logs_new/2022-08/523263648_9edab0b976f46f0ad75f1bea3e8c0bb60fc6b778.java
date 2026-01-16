package fr.kaibee;

import java.math.BigDecimal;

public record Amount(int valueInCents) implements Comparable<Amount> {
    public static Amount of(String value) {
        int valueInCents = new BigDecimal(value)
                .multiply(BigDecimal.valueOf(100))
                .intValue();
        return new Amount(valueInCents);
    }

    public Amount remove(Amount amount) {
        return new Amount(valueInCents() - amount.valueInCents());
    }

    public boolean canRemove(Amount amount) {
        return valueInCents() - amount.valueInCents() >= 0;
    }

    @Override
    public int compareTo(Amount o) {
        return Integer.compare(valueInCents, o.valueInCents);
    }
}