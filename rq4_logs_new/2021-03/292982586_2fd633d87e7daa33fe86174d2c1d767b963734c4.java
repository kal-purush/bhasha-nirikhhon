package com.uj.study.shopping_list;

import lombok.Getter;

/**
 * @author ：UncleJet
 * @date ：Created in 2021/3/1 上午11:52
 * @description：
 */
@Getter
public class Goods {
    private int price;
    private int weight;
    private int type;

    public Goods(int price, int weight, int type) {
        this.price = price;
        this.weight = weight;
        this.type = type;
    }
}