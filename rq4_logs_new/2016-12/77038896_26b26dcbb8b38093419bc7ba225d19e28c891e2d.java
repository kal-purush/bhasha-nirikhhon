package com.codingblocks.codequiz.models;

import com.codingblocks.codequiz.dummy_utils.DummyQuestions;
import com.codingblocks.codequiz.dummy_utils.DummyUser;

import java.util.ArrayList;
import java.util.Date;

/**
 * Created by piyush0 on 30/12/16.
 */
public class Challenge {

    private ArrayList<User> usersInGame;
    private ArrayList<Double> score;
    private ArrayList<Boolean> statuses;
    private Date date;
    private String topic;
    private Integer challenge_Id;
    private ArrayList<Question> questions;



    public Challenge(ArrayList<User> usersInGame,
                     ArrayList<Double> score, ArrayList<Boolean> pendingList, Date date, String topic, Integer challenge_Id, ArrayList<Question> questions) {
        this.usersInGame = usersInGame;
        this.score = score;
        this.statuses = pendingList;
        this.date = date;
        this.topic = topic;
        this.challenge_Id = challenge_Id;
        this.questions = questions;
    }


    public static ArrayList<Challenge> getDummyChallenges() {

        ArrayList<Challenge> challenges = new ArrayList<>();

        ArrayList<Boolean> statuses = new ArrayList<>();
        for (int i = 0; i < DummyUser.getUsers().size(); i++) {
            if (i % 2 == 0) {
                statuses.add(true);
            } else {
                statuses.add(false);
            }
        }

        ArrayList<Double> scores = new ArrayList<>();

        scores.add(11.2);
        scores.add(43.1);
        scores.add(5.3);
        scores.add(23.5);
        scores.add(16.4);
        scores.add(99.4);


        Challenge challenge1 = new Challenge(DummyUser.getUsers(), scores, statuses, new Date(2016, 2, 3), "C++", 0, DummyQuestions.getDummyQuestions());


        User user1 = new User("A", 23, "fns");
        User user2 = new User("B", 24, "fnsfdsf");

        ArrayList<User> users = new ArrayList<>();
        users.add(user1);
        users.add(user2);


        challenges.add(challenge1);

        challenges.add(challenge1);

        challenges.add(challenge1);

        challenges.add(challenge1);

        challenges.add(challenge1);

        return challenges;
    }

}