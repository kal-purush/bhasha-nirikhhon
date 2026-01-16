package ru.geekbrains;

import java.util.HashMap;
import java.util.Map;

public class DiscountMiddleware extends Middleware {
    private final Map<Long, Double> personalDiscounts = new HashMap<>();

    public DiscountMiddleware() {
        personalDiscounts.put(1L, 0.5);
        personalDiscounts.put(2L, 0.0);
        personalDiscounts.put(3L, 10.0);
    }

    public boolean check(Request request) {
        if (!personalDiscounts.get(request.getClientId()).equals(request.getDiscount())) {
            System.out.println("WRONG DISCOUNT!");
            return false;
        } else {
            return checkNext(request);
        }
    }
}