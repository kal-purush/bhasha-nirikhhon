package com.leechangu.sweettask;

import java.util.List;

/**
 * Created by Administrator on 2015/11/16.
 */
public class TaskParseDB {
    private static TaskParseDB ourInstance = new TaskParseDB();
    private String userName;
    private String password;

    public void setUserNameAndPassword(String userName, String password) {
        this.userName = userName;
        this.password = password;
    }

    public void saveTaskToParse(TaskItem taskItem) {
        // append userId to the taskItem, save to parse
    }

    public List<TaskItem> loadAllTasks() {
        // query all tasks that belong to username
        return null;
    }


    public static TaskParseDB getInstance() {
        return ourInstance;
    }

    private TaskParseDB() {
    }
}