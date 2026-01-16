package com.ciaocollect.trendmovies;

import android.util.Log;

import org.json.JSONObject;

public class Watch {
    private String id;
    private String photoList;
    private String photoPathList;
    private String titleList;
    private String scoreList;
    private String synopsisList;

    public Watch() { }

    public String getId() {
        return id;
    }

    public String getPhotoList() {
        return photoList;
    }

    public String getPhotoPathList() {
        return photoPathList;
    }

    public String getTitleList() {
        return titleList;
    }

    public String getScoreList() {
        return scoreList;
    }

    public String getSynopsisList() {
        return synopsisList;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setPhotoList(String photoList) {
        this.photoList = photoList;
    }

    public void setPhotoPathList(String photoPathList) {
        this.photoPathList = photoPathList;
    }

    public void setTitleList(String titleList) {
        this.titleList = titleList;
    }

    public void setScoreList(String scoreList) {
        this.scoreList = scoreList;
    }

    public void setSynopsisList(String synopsisList) {
        this.synopsisList = synopsisList;
    }

    public void watchListMovie(JSONObject object){
        try{
            String id = object.getString("id");
            String photoPath = object.getString("poster_path");
            String title = object.getString("title");
            String score = object.getString("vote_average");
            String synopsis = object.getString("overview");
            this.id = id;
            this.photoPathList = photoPath;
            this.titleList = title;
            this.scoreList = score;
            this.synopsisList = synopsis;
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}