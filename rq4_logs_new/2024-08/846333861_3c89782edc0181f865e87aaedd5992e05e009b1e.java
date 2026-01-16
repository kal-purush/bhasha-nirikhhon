public class Task {
    private String name;
    private boolean isDone;

    public Task(String name) {
        this.name = name;
        this.isDone = false;  // 初始状态为未完成
    }

    public void markAsDone() {
        this.isDone = true;
    }

    public void markAsNotDone() {
        this.isDone = false;
    }

    public String getStatusIcon() {
        return (isDone ? "[X]" : "[ ]"); // 返回状态图标，完成为[X]，未完成为[ ]
    }

    @Override
    public String toString() {
        return getStatusIcon() + " " + name;  // 返回带状态图标的任务字符串
    }
}