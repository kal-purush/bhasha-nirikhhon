package com.smalik.sftpasservice;

import java.util.Date;

public class FileInfo {

    private boolean directory;
    private String name;
    private Date creationTime;
    private long size;

    public FileInfo(String name, Date creationTime, boolean directory, long size) {
        this.name = name;
        this.creationTime = creationTime;
        this.directory = directory;
        this.size = size;
    }

    public boolean isDirectory() {
        return directory;
    }

    public String getName() {
        return name;
    }

    public Date getCreationTime() {
        return creationTime;
    }

    public long getSize() {
        return size;
    }
}