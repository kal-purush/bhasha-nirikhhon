package nju.joytrip.entity;

import cn.bmob.v3.BmobObject;
import cn.bmob.v3.BmobUser;

public class Comment extends BmobObject {
    private String content;
    private Event event;
    private User user;

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public Event getEvent() {
        return event;
    }

    public void setEvent(Event event) {
        this.event = event;
    }

    public User getUser() {
        return user;
    }

    public void setUser(User user) {
        this.user = user;
    }
}