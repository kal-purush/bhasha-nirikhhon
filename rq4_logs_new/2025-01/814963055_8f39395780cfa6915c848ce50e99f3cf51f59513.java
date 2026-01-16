package com.example.multithreadingProject.StackOverflow.components;

import com.example.multithreadingProject.StackOverflow.models.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class StackOverflow {
    private final Map<Integer, User> users;
    private final Map<Integer, Question> questions;
    private final Map<String, Tag> tags;
    private final Map<Integer, Answer> answers;

    public StackOverflow () {
        users = new ConcurrentHashMap<>();
        questions = new ConcurrentHashMap<>();
        answers = new ConcurrentHashMap<>();
        tags = new ConcurrentHashMap<>();
    }

    public User createUser(String username, String email) {
        int id = users.size() + 1;
        User user = new User(id, username, email);
        users.put(id, user);
        return user;
    }

    public Question askQuestion(User author, String title, String content, List<String> tags) {
        Question question = author.askQuestion(title, content, tags);
        questions.put(question.getId(), question);
        for(Tag tag: question.getTags()) {
            this.tags.putIfAbsent(tag.getValue(), tag);
        }

        return question;
    }

    public Answer answerQuestion(User author, Question question, String content) {
        Answer answer = author.answerQuestion(question, content);
        answers.put(answer.getId(), answer);
        return answer;
    }

    public Comment addComment(User author, Commentable commentable, String content) {
        Comment comment = author.addComment(commentable, content);
        return comment;
    }

    public void voteQuestion(User user, Question question, int value) {
        question.vote(user, value);
    }

    public void voteAnswer(User user, Answer answer, int value) {
        answer.vote(user, value);
    }

    public void acceptAnswer(Answer answer) {
        answer.markAccepted();
    }

    public List<Question> searchQuestions(String query) {
        return questions.values().stream()
                .filter(q -> q.getTitle().toLowerCase().contains(query.toLowerCase()) ||
                        q.getContent().toLowerCase().contains(query.toLowerCase()) ||
                        q.getTags().stream().anyMatch(t -> t.getValue().equalsIgnoreCase(query)))
                .collect(Collectors.toList());
    }

    public List<Question> getQuestionsByUser(User user) {
        return user.getQuestions();
    }

}