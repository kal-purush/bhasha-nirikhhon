package fr.openent.minibadge.core.constants;

public class Actions {
    public static final String BADGE_ASSIGN  = "BADGE_ASSIGN";
    public static final String BADGE_ASSIGN_REVOKE  = "BADGE_ASSIGN_REVOKE";
    public static final String BADGE_PUBLISH  = "BADGE_PUBLISH";
    public static final String BADGE_PRIVATIZE  = "BADGE_PRIVATIZE";
    public static final String BADGE_REFUSE  = "BADGE_REFUSE";
    public static final String CHART_ACCEPT  = "BADGE_REFUSE";
    public static final String CHART_REFUSE  = "BADGE_REFUSE";


    private Actions() {
        throw new IllegalStateException("Utility class");
    }
}
