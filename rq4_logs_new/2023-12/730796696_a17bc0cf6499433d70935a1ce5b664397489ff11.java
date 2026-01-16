package currency;

public enum Precision {
    TWO(2),
    THREE(3),
    FOUR(4);

    private final int value;
    Precision(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}