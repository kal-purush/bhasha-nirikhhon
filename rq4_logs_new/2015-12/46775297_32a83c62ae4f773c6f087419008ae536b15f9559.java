package com.group33.greenthumb;

import com.activeandroid.Model;
import com.activeandroid.annotation.Column;
import com.activeandroid.annotation.Table;
import com.activeandroid.query.Select;

import java.util.Calendar;
import java.util.List;

@Table(name = "Tasks")
public class Task extends Model {
    // NOTE: If querying the database causes your app to crash because of a SQLiteException, try uninstalling and reinstalling/rerunning the app

    @Column(name = "Name")
    public String name;
    public int type;
    public static final int OTHER = 0;
    public static final int WATER = 1;
    public static final int HARVEST = 2;
    public static final int COMPOST = 3;

    public Task(){
        super();
    }

    public Task(String name) {
        super();
        this.name = name;
        type = OTHER;
    }

    public static Task getTask(String name) {
        return new Select()
                .from(Task.class)
                .where("Name = ?", name)
                .executeSingle();
    }

    public static List<Task> getAllTasks() {
        return new Select(new String[]{"Id, Name"})
                .from(Task.class)
                .orderBy("Name ASC")
                .execute();
    }

    public static List<Task> search(String searchTerm) {
        return new Select().from(Task.class)
                .where("Name LIKE ?", new String[]{'%' + searchTerm + '%'})
                .orderBy("Name ASC")
                .execute();
    }
}