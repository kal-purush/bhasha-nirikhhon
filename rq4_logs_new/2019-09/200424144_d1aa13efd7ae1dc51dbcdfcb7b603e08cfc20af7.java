package com.kodilla.stream.forumuser;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class Forum {
    private final List<ForumUser> theForumUsers = new ArrayList<>();

    public Forum() {
        int n = 0;
        char sex = 'x';
        for (n = 1; n <= 20; n++) {
            int uniqueUserID = n;
            String userName = "User" + n;
            if (n % 2 == 0) {
                sex = 'F';
            } else {
                sex = 'M';
            }
            LocalDate birthDate = LocalDate.parse(1989 + n + "-" + 11 + "-" + n);
            int numberOfPostPublished = theForumUsers.size();
            theForumUsers.add(new ForumUser(uniqueUserID, userName, sex, birthDate, numberOfPostPublished));
        }
        public List<ForumUser> getUserList() {
            return new ArrayList<>(theForumUsers);
        }
    }
}