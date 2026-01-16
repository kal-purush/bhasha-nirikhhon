package np.com.naxa.factsnepal.notification;

import java.util.ArrayList;

public class FactsNotification {
    private String title;
    private String icon;
    private String time;
    private boolean isRead;

    public FactsNotification(String title, String icon, String time, boolean isRead) {
        this.title = title;
        this.icon = icon;
        this.time = time;
        this.isRead = isRead;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getIcon() {
        return icon;
    }

    public void setIcon(String icon) {
        this.icon = icon;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public boolean isRead() {
        return isRead;
    }

    public void setRead(boolean read) {
        isRead = read;
    }

    @Deprecated
    public static ArrayList<FactsNotification> getDemoItems() {
        ArrayList<FactsNotification> list = new ArrayList<>();

        list.add(new FactsNotification("Ncell survey in customer research", "", "Just now",false));
        list.add(new FactsNotification("Kantipur television survey on service", "", "12m ago",false));
        list.add(new FactsNotification("Royal Singi age group survey", "", "1 day ago",true));
        list.add(new FactsNotification("Ncell survey in customer research", "", "10 Feb, 2019",true));

        return list;
    }
}