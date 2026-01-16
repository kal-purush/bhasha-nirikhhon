package emotionmining.model;

/**
 * Created by dhruv on 05/06/17.
 */
public enum Labels {
    HAPPY(0),
    SAD(1),
    ANGER(2),
    SURPRISE(3),
    LOVE(4),
    DISGUST(5),
    TRUST(6),
    FEAR(7);

    private int value;

    private Labels(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static Labels fromString(String label) {
        if (label.equalsIgnoreCase("HAPPY")) {
            return HAPPY;
        } else if (label.equalsIgnoreCase("SAD")) {
            return SAD;
        } else if (label.equalsIgnoreCase("ANGER")) {
            return ANGER;
        } else if (label.equalsIgnoreCase("SURPRISE")) {
            return SURPRISE;
        } else if (label.equalsIgnoreCase("LOVE")) {
            return LOVE;
        } else if (label.equalsIgnoreCase("DISGUST")) {
            return DISGUST;
        } else if (label.equalsIgnoreCase("TRUST")) {
            return TRUST;
        } else if (label.equalsIgnoreCase("FEAR")) {
            return FEAR;
        } else return null;
    }

    public String toString() {
        switch (value) {
            case 0:
                return "HAPPY";
            case 1:
                return "SAD";
            case 2:
                return "ANGER";
            case 3:
                return "SURPRISE";
            case 4:
                return "LOVE";
            case 5:
                return "DISGUST";
            case 6:
                return "TRUST";
            case 7:
                return "FEAR";
            default:
                return "";
        }
    }

    private static final int size = Labels.values().length;

    public static int getSize() {
        return size;
    }
}