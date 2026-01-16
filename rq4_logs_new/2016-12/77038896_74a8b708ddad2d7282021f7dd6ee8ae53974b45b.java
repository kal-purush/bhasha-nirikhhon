package com.codingblocks.codequiz.dummy_utils;

import com.codingblocks.codequiz.models.Challenge;
import com.codingblocks.codequiz.models.ChallengeSmall;
import com.codingblocks.codequiz.models.User;

import java.util.ArrayList;
import java.util.Date;

/**
 * Created by piyush0 on 30/12/16.
 */
public class DummyChallenge {

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

        scores.add(1.1);
        scores.add(2.2);
        scores.add(3.3);
        scores.add(23.5);
        scores.add(16.4);
        scores.add(99.4);


        Challenge challenge1 = new Challenge(DummyUser.getUsers(), scores, statuses, new Date(2016, 2, 3), "C++", 0, DummyQuestions.getDummyQuestions());


        challenges.add(challenge1);

        challenges.add(challenge1);

        challenges.add(challenge1);

        challenges.add(challenge1);

        challenges.add(challenge1);

        return challenges;
    }

    public static ArrayList<ChallengeSmall> getDummyChallengeSmalls() {
        ArrayList<ChallengeSmall> challengeSmalls = new ArrayList<>();
        ArrayList<String> userNames = new ArrayList<>();
        ArrayList<User> users = DummyUser.getUsers();

        for(int i = 0 ;i<users.size(); i++){
            userNames.add(users.get(i).getName());
        }


        ChallengeSmall challenge1 = new ChallengeSmall(0, "C++",  new Date(2016, 2, 3),userNames);
        challengeSmalls.add(challenge1);
        challengeSmalls.add(challenge1);
        challengeSmalls.add(challenge1);
        challengeSmalls.add(challenge1);
        challengeSmalls.add(challenge1);

        return challengeSmalls;
    }

}