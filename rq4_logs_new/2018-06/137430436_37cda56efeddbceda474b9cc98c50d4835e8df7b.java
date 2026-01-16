package com.migu.schedule.info;

import java.util.ArrayList;
import java.util.List;

public class Node {
    /**
     * 服务器编号
     */
    private int id;

    /**
     * 服务器上运行的任务集合
     */
    private List<Task> mTaskList = new ArrayList<Task>();

    /**
     *  获得该服务器上的总消耗率
     * @return
     */
    public int getConsumption(){
        int consumption = 0;
        for(int i=0;i<mTaskList.size();i++){
            Task task = mTaskList.get(i);
            consumption += task.getConsumption();
        }
        return consumption;
    }

    public void addTask(Task task){
        if(task != null) {
            mTaskList.add(task);
        }
    }

    public void deleteTask(int taskId){
        int index = -1;
        for(int i=0;i<mTaskList.size();i++){
            Task task = mTaskList.get(i);
            if(task.getId() == taskId){
                index = i;
                break;
            }
        }
        if(index != -1){
            mTaskList.remove(index);
        }
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public List<Task> getmTaskList() {
        return mTaskList;
    }
}