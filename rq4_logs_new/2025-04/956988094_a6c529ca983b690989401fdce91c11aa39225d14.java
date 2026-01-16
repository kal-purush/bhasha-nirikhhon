package com.prime.models.response;

import lombok.Data;
import java.util.List;

@Data
public class ProjectOverallStatisticsResponse {
    private long totalProjects;
    private long totalTasks;
    private List<ProjectTaskStatusStats> projectStats;

    @Data
    public static class ProjectTaskStatusStats {
        private String projectName;
        private long totalTasks;
        private double completionPercentage;
        private TaskStatusBreakdown statusBreakdown;
    }

    @Data
    public static class TaskStatusBreakdown {
        private long backlogTasks;
        private long doingTasks;
        private long onHoldTasks;
        private long doneTasks;
        private long archivedTasks;
        
        private double backlogPercentage;
        private double doingPercentage;
        private double onHoldPercentage;
        private double donePercentage;
        private double archivedPercentage;
    }
} 