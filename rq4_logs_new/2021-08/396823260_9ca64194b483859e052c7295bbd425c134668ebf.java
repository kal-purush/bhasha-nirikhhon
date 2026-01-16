package design_patterns.adapter;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ProductHandlerAdapter {
    private final ProductHandler productHandler;

    private Pricable convertCostableToPricable(Costable costable) {
        return new Pricable() {
            @Override
            public int getPrice() {
                return costable.cost();
            }
        };
    }

    public void handle(Costable costable) {
        productHandler.handle(convertCostableToPricable(costable));
    }
}