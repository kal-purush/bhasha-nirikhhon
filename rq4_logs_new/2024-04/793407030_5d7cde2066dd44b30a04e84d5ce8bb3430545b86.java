package hr.algebra.model;

public enum Grade {
    EXELLENTH(5),
    VERY_GOOD(4),
    GOOD(3),
    SUFFICIENT(2),
    FAIL(1);

    private final int grade;

    private Grade(int grade) {
        this.grade = grade;
    }

    public int getGrade() {
        return grade;
    }

    public static Grade from(int grade) {
        for (Grade value : values()) {
            if (value.grade == grade) {
                return value;
            }
        }
        throw new RuntimeException("no such grade");
    }

}