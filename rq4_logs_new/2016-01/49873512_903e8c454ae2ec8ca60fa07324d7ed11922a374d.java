package com.wastedtimecounter.realm;

import io.realm.RealmObject;
import io.realm.annotations.PrimaryKey;

/**
 * Realm object.
 * Which save Applications data in db.
 * <p/>
 * Created by Eugene on 1/26/2016.
 */
public class ApplicationRealm extends RealmObject {

    @PrimaryKey
    private String packageName;
    private String appName;

    private long secondsCount;

    /*
    |---------------------------------------------
    | Setters...
    |---------------------------------------------
     */

    public void setPackageName(String packageName) {
        this.packageName = packageName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public void setSecondsCount(long secondsCount) {
        this.secondsCount = secondsCount;
    }

    /*
    |---------------------------------------------
    | Getters...
    |---------------------------------------------
    */

    public String getPackageName() {
        return packageName;
    }

    public String getAppName() {
        return appName;
    }

    public long getSecondsCount() {
        return secondsCount;
    }
}