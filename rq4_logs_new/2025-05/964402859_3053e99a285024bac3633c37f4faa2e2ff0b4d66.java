package com.fatepet.petrest.funeralproduct;

public enum PriceType {

    FREE("무료"),
    CONTACT("직접문의"),
    MANUAL("직접입력");

    private final String displayName;

    PriceType(String displayName) {
        this.displayName = displayName;
    }

    public String getDisplayName() {
        return displayName;
    }

    public static PriceType fromDisplayName(String displayName) {
        for (PriceType priceType : PriceType.values()) {
            if (priceType.getDisplayName().equals(displayName)) {
                return priceType;
            }
        }
        throw new IllegalArgumentException("priceType 형식 오류: " + displayName);
    }
}