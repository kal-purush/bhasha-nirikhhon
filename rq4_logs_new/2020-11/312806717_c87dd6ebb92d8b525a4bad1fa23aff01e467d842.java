package com.github.Rasen0000;

import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;

public class ATMMoney<currency> extends ATM {
    String currency;
    @Getter
    @Setter
    private BigDecimal ATMMoney; //запас денег в банкомате

    @Getter
    @Setter
    private BigDecimal money; //деньги требуемые клиенту

    public void getMoney() {
        ATMMoney = ATMMoney.subtract(money);
        //System.out.println("Выдача средств " + money +" "+ currency );
        //System.out.println("Остаток средств в банкомате " + ATMMoney +" "+ currency);

    }
}