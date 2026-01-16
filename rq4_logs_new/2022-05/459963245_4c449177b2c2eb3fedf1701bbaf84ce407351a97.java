package es.ucm.arblemar.gamelogic.enums;

public enum FadeAnimation {
    NONE(0),
    TITLE_HINT(1),
    TITLE_UNDO(2),
    UNDO_HINT(3);
    ;

    private FadeAnimation(int i) {
        this.value = i;
    }

    public int getValue() {
        return value;
    }

    private final int value;
}