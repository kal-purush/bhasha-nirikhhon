package cs378.bracketapp;

import android.app.Application;

import java.util.Map;

/**
 * Created by Development on 5/3/15.
 */
public class GlobalState extends Application {
    private String uid;
    private Map<String, Object> userBrackets;

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public Map<String, Object> getUserBrackets() {
        return userBrackets;
    }

    public void setUserBrackets(Map<String, Object> userBrackets) {
        this.userBrackets = userBrackets;
    }

}