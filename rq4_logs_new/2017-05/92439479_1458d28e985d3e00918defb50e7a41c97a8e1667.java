package fiuba.aninfo.entity;

import java.lang.String;
import java.util.List;
import java.util.ArrayList;

public class Card {

    private long id;
    private long estimation; // In Hs
    private String title;
    private String description;

    private Classification classification;

    private List<User> assignees;

    public Card() {
        assignees = new ArrayList<User>();
        classification = Classification.INTERNAL;
    }

    public void setId(long id) {
        this.id = id;
    }

    public void setEstimation(long hs) {
        this.estimation = hs;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void setClassification(Classification classification) {
        this.classification = classification;
    }
    
    public void addAssignee(User user) {
        if (!assignees.contains(user)) {
            assignees.add(user);
        }
    }

    public boolean removeAssignee(User user) {
        return assignees.remove(user);
    }

    public Classification getClassification() {
        return classification;
    }

    public boolean isAssignee(User user) {
        return assignees.contains(user);
    }

    public List<User> getAssignees() {
        return new ArrayList<User>(assignees);
    }

    public long getId() {
        return id;
    }

    public long getEstimation() {
        return estimation;
    }

    public String getTitle() {
        return title;
    }

    public String getDescription() {
        return description;
    }

    public enum Classification {
        INTERNAL,
        EXTERNAL
    }

}