package groupone.userservice.entity;

public enum UserType {
    SUPER_ADMIN("SUPER_ADMIN"),
    ADMIN("ADMIN"),
    NORMAL_USER("NORMAL_USER"),
    NORMAL_USER_NOT_VALID("NORMAL_USER_NOT_VALID"),
    VISITOR_BANNED("VISITOR_BANNED");

    public final String label;

    UserType(String label) {
        this.label = label;
    }

    public static String getLabelFromOrdinal(int ordinal) {
        for (UserType e : UserType.values()) {
            if (e.ordinal() == ordinal) return e.label;
        }
        return null;
    }
}