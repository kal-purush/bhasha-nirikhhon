package model;

import java.util.Date;

/**
 * Created by Asif on 3/17/2018.
 */

public class ContributedItem {

    String trailstationid,learningtrailid,userid,fileurl,filedescription;
    Date timestamp;
    int itemid;
    public ContributedItem() {
    }

    public String getTrailStationID() {
        return trailstationid;
    }

    public void setTrailStationID(String trailstationid) {
        this.trailstationid = trailstationid;
    }



    public String getLearningTrailID() {
        return learningtrailid;
    }

    public void setLearningTrailID(String learningtrailid) {
        this.learningtrailid = learningtrailid;
    }

    public String getFileUrl() {
        return fileurl;
    }

    public void setFileUrl(String fileurl) {
        this.fileurl = fileurl;
    }


    public String getFileDescription() {
        return filedescription;
    }

    public void setFileDescription(String filedescription) {
        this.filedescription = filedescription;
    }


    public String getUserID() {
        return userid;
    }

    public void setUserID(String userid) {
        this.userid = userid;
    }



    public int getItemID() {
        return itemid;
    }

    public void setItemID(int itemid) {
        this.itemid = itemid;
    }


    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

}