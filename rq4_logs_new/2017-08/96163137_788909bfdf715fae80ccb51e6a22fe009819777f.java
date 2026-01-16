package com.yundian.celebrity.event;

/**
 * eventBus消息类
 * Created by Administrator on 2017/4/4.
 */
public class VideoMessageEvent {
    private final String url;
    private int position;
    public VideoMessageEvent(int position, String url) {
        this.position = position;
        this.url = url;
    }

    public String getUrl() {
        return url;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }
}