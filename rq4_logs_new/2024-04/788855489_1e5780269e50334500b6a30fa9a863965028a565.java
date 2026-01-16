package growthook.org.bamgang.members.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "finishedtrail")
public class FinishedWalk {

    @Id
    private int finishedId;

    private int trailId;

    private int userId;

    private String review;

    private String walkedDate;

    public int getFinishedId() {
        return finishedId;
    }

    public void setFinishedId(int finishedId) {
        this.finishedId = finishedId;
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        userId = userId;
    }

    public String getWalkedDate() {
        return walkedDate;
    }

    public void setWalkedDate(String walkedDate) {
        this.walkedDate = walkedDate;
    }

    public String getReview() {
        return review;
    }

    public void setReview(String review) {
        this.review = review;
    }

    public int getTrailId() {
        return trailId;
    }

    public void setTrailId(int trailId) {
        this.trailId = trailId;
    }
}