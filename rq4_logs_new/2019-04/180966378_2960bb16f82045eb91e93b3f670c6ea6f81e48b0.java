package com.lidegui.littledrawer.bean;

import java.util.Date;

/**
 * @Author: lidegui
 * @Date:Created in 9:33 2019/4/16
 */
public class File {

    private int id;
    private String name;
    // 存放文件的类型，如png、mp4
    private String mime;
    //文件存放路径，本地或者网络
    private String path;
    private Date date;
    private int mediaId;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getMime() {
        return mime;
    }

    public void setMime(String mime) {
        this.mime = mime;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public int getMediaId() {
        return mediaId;
    }

    public void setMediaId(int mediaId) {
        this.mediaId = mediaId;
    }
}