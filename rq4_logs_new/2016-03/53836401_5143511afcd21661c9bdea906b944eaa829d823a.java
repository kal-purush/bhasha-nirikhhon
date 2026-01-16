package com.scrumtrek.simplestore.pricecodes;

/**
 * Created by pugach on 15/03/16.
 */
public class ChildrenCode extends PriceCode {

    /**
     * Возваращет значение цены ренты для текущего прайс кода
     * @param daysRented - число дней ренты
     * @return
     */
    @Override
    public double getAmount(int daysRented) {
        double amount = 1.5;
        if(daysRented > 3) amount = (daysRented-3)*1.5;
        return amount;
    }

    /**
     * Возвращает число дополнительных очков ренты для текущего прайс кода
     * @param daysRented - число дней ренты
     * @return
     */
    @Override
    public int getBonus(int daysRented) {
        return 1;
    }
}