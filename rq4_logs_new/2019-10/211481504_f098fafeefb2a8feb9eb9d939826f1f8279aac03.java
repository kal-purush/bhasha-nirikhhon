package litequest.com.litetask.dto;

import com.fasterxml.jackson.annotation.JsonView;
import litequest.com.litetask.domain.Task;
import litequest.com.litetask.domain.views.Views;

import java.util.List;

@JsonView(Views.Full.class)
public class TaskPageDto {
    private List<Task> tasks;
    private int currentPage;
    private int totalPage;

    public TaskPageDto() {
    }

    public TaskPageDto(List<Task> tasks, int currentPage, int totalPage) {
        this.tasks = tasks;
        this.currentPage = currentPage;
        this.totalPage = totalPage;
    }

    public List<Task> getTasks() {
        return tasks;
    }

    public void setTasks(List<Task> tasks) {
        this.tasks = tasks;
    }

    public int getCurrentPage() {
        return currentPage;
    }

    public void setCurrentPage(int currentPage) {
        this.currentPage = currentPage;
    }

    public int getTotalPage() {
        return totalPage;
    }

    public void setTotalPage(int totalPage) {
        this.totalPage = totalPage;
    }
}