package com.br.javaecommerce.enums;

public enum BillPaymentStatus {
    PAYED("Quitada"),
    OPENED("Aberta"),
    EXPIRED("Vencida"),
    RENEWED("Renegociada"),
    NOPAYED("À pagar");

    private final String description;

    BillPaymentStatus(String description) {
        this.description = description;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public String toString() {
        return description;
    }
}