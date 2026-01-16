package com.company.getyourgoal.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import javax.persistence.*;
import java.io.Serializable;
import java.util.Objects;

    @Entity
    @JsonIgnoreProperties({"hibernateLazyInitializer", "handler"})
    @Table(name = "goal")
    public class Goal implements Serializable {
        @Id
        @GeneratedValue(strategy = GenerationType.AUTO)
        private Integer id;
        private String goalTitle;
        private String goal;
        private String userName;
        private Integer userId;

        public Goal() {
        }

        public Goal(Integer id, String goalTitle, String goal, String userName, Integer userId) {
            this.id = id;
            this.goalTitle = goalTitle;
            this.goal = goal;
            this.userName = userName;
            this.userId = userId;
        }

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }

        public String getGoalTitle() {
            return goalTitle;
        }

        public void setGoalTitle(String goalTitle) {
            this.goalTitle = goalTitle;
        }

        public String getGoal() {
            return goal;
        }

        public void setGoal(String goal) {
            this.goal = goal;
        }

        public String getUserName() {
            return userName;
        }

        public void setUserName(String userName) {
            this.userName = userName;
        }

        public Integer getUserId() {
            return userId;
        }

        public void setUserId(Integer userId) {
            this.userId = userId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Goal goal1 = (Goal) o;
            return Objects.equals(id, goal1.id) && Objects.equals(goalTitle, goal1.goalTitle) && Objects.equals(goal, goal1.goal) && Objects.equals(userName, goal1.userName) && Objects.equals(userId, goal1.userId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, goalTitle, goal, userName, userId);
        }

        @Override
        public String toString() {
            return "Goal{" +
                    "id=" + id +
                    ", goalTitle='" + goalTitle + '\'' +
                    ", goal='" + goal + '\'' +
                    ", userName='" + userName + '\'' +
                    ", userId=" + userId +
                    '}';
        }
    }

