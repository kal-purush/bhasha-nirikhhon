package com.example.mojing;

public class VIP_Designer_InfoType {
    private String name;
    private String introduction;
    private int avatarResId; // 头像资源 ID

    public VIP_Designer_InfoType(String name, String introduction, int avatarResId) {
        this.name = name;
        this.introduction = introduction;
        this.avatarResId = avatarResId;
    }

    public String getName() {
        return name;
    }

    public String getIntroduction() {
        return introduction;
    }

    public int getAvatarResId() {
        return avatarResId;
    }
}