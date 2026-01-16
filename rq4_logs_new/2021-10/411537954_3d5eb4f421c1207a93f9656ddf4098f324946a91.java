package cooper.resources;

import cooper.finance.FinanceManager;
import cooper.meetings.MeetingManager;
import cooper.verification.UserRole;

public class ResourcesManager {

    private final FinanceManager cooperFinanceManager;
    private final MeetingManager cooperMeetingManager;

    public ResourcesManager(FinanceManager financeManager, MeetingManager meetingManager) {
        cooperFinanceManager = financeManager;
        cooperMeetingManager = meetingManager;
    }

    public FinanceManager getFinanceManager(UserRole userRole) {
        if (userRole.equals(UserRole.ADMIN)) {
            return cooperFinanceManager;
        } else {
            return null;
        }
    }

    public MeetingManager getMeetingManager(UserRole userRole) {
        return cooperMeetingManager;
    }
}