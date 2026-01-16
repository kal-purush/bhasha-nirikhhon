package com.hanhangproject.demo.entity;

public class PHome {
    private int id;
    private String userId;
    private int homeId;
    private String mode;
    private int reqScriptId;
    private boolean inside;
    private boolean pairing;
    private String createdAt;
    private String updateAt;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public int getHomeId() {
        return homeId;
    }

    public void setHomeId(int homeId) {
        this.homeId = homeId;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public int getReqScriptId() {
        return reqScriptId;
    }

    public void setReqScriptId(int reqScriptId) {
        this.reqScriptId = reqScriptId;
    }

    public boolean isInside() {
        return inside;
    }

    public void setInside(boolean inside) {
        this.inside = inside;
    }

    public String getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(String createdAt) {
        this.createdAt = createdAt;
    }

    public String getUpdateAt() {
        return updateAt;
    }

    public void setUpdateAt(String updateAt) {
        this.updateAt = updateAt;
    }

    public boolean isPairing() {
        return pairing;
    }

    public void setPairing(boolean pairing) {
        this.pairing = pairing;
    }
}