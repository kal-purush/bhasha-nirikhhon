package com.kodilla.testing.forum.statistics;

public class StatisticsCalculator {
    private int userCounter;
    private int postCounter;
    private int commentCounter;
    private double averagePostPerUser;
    private double averageCommentPerUser;
    private double averageCommentPerPost;

    public void calculateAdvStatistics(Statistics statistics) {
        userCounter = statistics.usersNames().size();
        postCounter = statistics.postsCount();
        commentCounter = statistics.commentsCount();
        if(userCounter == 0) {
            averagePostPerUser = 0;
            averageCommentPerUser = 0;
        } else {
            averagePostPerUser = (double) postCounter / (double) userCounter;
            averageCommentPerUser = (double) commentCounter / (double) userCounter;
        }
        if(postCounter == 0) {
            averageCommentPerPost = 0;
        } else {
            averageCommentPerPost = (double) commentCounter / (double) postCounter;
        }
    }

    public void showStatistics() {
        System.out.println("User counter: " + userCounter);
        System.out.println("Post counter: " + postCounter);
        System.out.println("Comment counter: " + commentCounter);
        System.out.println("Average posts per user: " + averagePostPerUser);
        System.out.println("Average comments per user:  " + averageCommentPerUser);
        System.out.println("Average comment per post: " + averageCommentPerPost);
    }

    public int getUserCounter() {
        return userCounter;
    }

    public int getPostCounter() {
        return postCounter;
    }

    public int getCommentCounter() {
        return commentCounter;
    }

    public double getAveragePostPerUser() {
        return averagePostPerUser;
    }

    public double getAverageCommentPerUser() {
        return averageCommentPerUser;
    }

    public double getAverageCommentPerPost() {
        return averageCommentPerPost;
    }
}