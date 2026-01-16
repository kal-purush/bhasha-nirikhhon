package com.kodilla.testing.forum.statistics;

import java.util.ArrayList;
import java.util.List;

public class ForumStatistics {

    private int usersQuantity;
    private int postsQuantity;
    private int commentsQuantity;
    private double avgPostPerUser;
    private double avgCommentsPerUser;
    private double avgCommentsPerPost;

    public ForumStatistics(Statistics statistics){}

    public int getUsersQuantity() {
        return usersQuantity;
    }

    public int getPostsQuantity() {
        return postsQuantity;
    }

    public int getCommentsQuantity() {
        return commentsQuantity;
    }

    public double getAvgPostPerUser() {
        return avgPostPerUser;
    }

    public double getAvgCommentsPerUser() {
        return avgCommentsPerUser;
    }

    public double getAvgCommentsPerPost() {
        return avgCommentsPerPost;
    }

    public void calculateAdvStatistics(Statistics statistics) {
        usersQuantity = statistics.usersNames().size();
        postsQuantity = statistics.postsCount();
        commentsQuantity = statistics.commentsCount();
        avgPostPerUser = calculateAvgPostPerUser();
        avgCommentsPerUser = calculateAvgCommentsPerUser();
        avgCommentsPerPost = calculateAvgCommentsPerPost();
    }

    private double calculateAvgPostPerUser() {
        if(usersQuantity > 0) {
            avgPostPerUser = postsQuantity / usersQuantity;
            return avgPostPerUser;
        } else {
            return 0;
        }
    }

    private double calculateAvgCommentsPerUser() {
        if(usersQuantity > 0) {
            avgCommentsPerUser = commentsQuantity / usersQuantity;
            return avgCommentsPerUser;
        } else {
            return 0;
        }
    }
    private double calculateAvgCommentsPerPost() {
        if(postsQuantity > 0) {
            avgCommentsPerPost = commentsQuantity / postsQuantity;
            return avgCommentsPerPost;
        } else {
            return 0;
        }
    }

    public void showStatistics() {
        System.out.println("Forum has " + usersQuantity + " users.");
        System.out.println("Forum users have added " + postsQuantity + " posts.");
        System.out.println("Forum users have commented posts " + commentsQuantity + " times.");
        System.out.println("Average number post per user equals " + avgPostPerUser + ".");
        System.out.println("Average number comments per user equals " + avgCommentsPerUser + ".");
        System.out.println("Average number comments per post equals" + avgCommentsPerPost + ".");
    }
}