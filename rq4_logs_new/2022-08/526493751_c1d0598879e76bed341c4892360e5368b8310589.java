package com.davidholcombe.discounts.domain;

import com.davidholcombe.discounts.repository.DiscountEntity;

public class DiscountFactory {

    public static Discount from(final DiscountEntity entity) {

        switch (entity.getType()) {

            case FOR_ITEM_TYPE:
                return DiscountForItemType.from(entity);

            case FOR_COUNT:
                return DiscountForItemCount.from(entity);

            case FOR_COST:
                return DiscountForItemCost.from(entity);

            default:
                // TODO change exception type
                throw new RuntimeException(String.format("Unknown discount type: %s", entity.getType()));
        }
    }
}