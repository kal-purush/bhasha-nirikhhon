package io.juismi;
import java.util.Date;

/**
 * Created by Ismael on 22/02/2018.
 */

public class CommentModel {
    String userName;
    String id;
    String comment;
    Date date;

    public CommentModel(String userName, String id, String comment){
        this.userName = userName;
        this.id = id;
        this.comment = comment;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }
}