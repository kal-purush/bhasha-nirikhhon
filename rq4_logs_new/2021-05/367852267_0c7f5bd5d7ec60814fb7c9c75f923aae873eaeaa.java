package com.example.taskmaster;

import androidx.room.Dao;
import androidx.room.Insert;
import androidx.room.Query;

import java.util.ArrayList;
import java.util.List;
//interface
@Dao
public interface DAOsTask {
    @Query("SELECT * FROM task")
    ArrayList<Task> getAll();

//    @Query("SELECT * FROM task WHERE taskId IN (:userIds)")
//    List<Task> loadAllByIds(int[] userIds);
//
//    @Query("SELECT * FROM task WHERE title LIKE :first AND " +
//            "body LIKE :last LIMIT 1")
//
    @Insert
    void insertAll(Task task);
//
//    @Delete
//    void delete(Task user);
}