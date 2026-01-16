package byuecel.labor.tarkovhelper.model.hideout;

import byuecel.labor.tarkovhelper.model.item.ContainedItem;

public class Craft {
    private int id;
    private Station station;
    private int level;
    private Task taskUnlock;
    private float duration;
    private ContainedItem[] requiredItems;
    private ContainedItem[] rewardItems;

    public Craft() {
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public Station getStation() {
        return station;
    }

    public void setStation(Station station) {
        this.station = station;
    }

    public int getLevel() {
        return level;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    public Task getTaskUnlock() {
        return taskUnlock;
    }

    public void setTaskUnlock(Task taskUnlock) {
        this.taskUnlock = taskUnlock;
    }

    public float getDuration() {
        return duration;
    }

    public void setDuration(float duration) {
        this.duration = duration;
    }

    public ContainedItem[] getRequiredItems() {
        return requiredItems;
    }

    public void setRequiredItems(ContainedItem[] requiredItems) {
        this.requiredItems = requiredItems;
    }

    public ContainedItem[] getRewardItems() {
        return rewardItems;
    }

    public void setRewardItems(ContainedItem[] rewardItems) {
        this.rewardItems = rewardItems;
    }
}