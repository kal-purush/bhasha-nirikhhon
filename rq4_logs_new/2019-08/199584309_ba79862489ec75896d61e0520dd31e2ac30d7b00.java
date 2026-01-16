package com.kodilla.stream.forumuser;

import java.time.LocalDate;

public final class ForumUser {
    private final int identityNumber;
    private final String userName;
    private final char sex;
    private final LocalDate dayOfBirth;
    private final int numberOfPosts;

    public ForumUser(final int identityNumber, final String userName, final char sex, final LocalDate dayOfBirth, final int numberOfPosts) {
        this.identityNumber = identityNumber;
        this.userName = userName;
        this.sex = sex;
        this.dayOfBirth = dayOfBirth;
        this.numberOfPosts = numberOfPosts;
    }

    public int getIdentityNumber() {
        return identityNumber;
    }

    public String getUserName() {
        return userName;
    }

    public char getSex() {
        return sex;
    }

    public LocalDate getDayOfBirth() {
        return dayOfBirth;
    }

    public int getNumberOfPosts() {
        return numberOfPosts;
    }

    @Override
    public String toString() {
        return "ForumUser{" +
                "identityNumber=" + identityNumber +
                ", userName='" + userName + '\'' +
                ", sex=" + sex +
                ", dayOfBirth=" + dayOfBirth +
                ", numberOfPosts=" + numberOfPosts +
                '}';
    }
}