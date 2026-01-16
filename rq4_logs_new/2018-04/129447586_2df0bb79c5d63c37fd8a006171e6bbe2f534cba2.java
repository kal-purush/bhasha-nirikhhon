package sample;

public class Assignment {
    Task task;
    Resource resource;
    Manager manager;
    double progress = 0;

    public Assignment(Task task, Resource resource, Manager manager)
    {
        this.task = task;
        this.resource = resource;
        this.manager = manager;
    }
    Resource getResource()
    {
        return  this.resource;
    }
    Task getTask()
    {
        return  this.getTask();
    }
    void setProgress(Double progress)
    {
        this.progress = progress;
    }
    double getProgress()
    {
        return  this.progress;
    }
}