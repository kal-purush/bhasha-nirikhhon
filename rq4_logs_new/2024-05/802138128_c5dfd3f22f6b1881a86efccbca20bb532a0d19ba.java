package com.example.study;

public class ChemistryQuiz {

    private final String[] questions = {
            "What is the symbol for sodium?",
            "What is the basic unit of mass in the SI system?",
            "What is the process of converting a gas into a liquid?",
            "Which of the following is an alkali metal?",
            "What is the formula for water?"
    };

    private final String[] answers = {
            "Na",
            "gram",
            "condensation",
            "lithium",
            "H2O"
    };

    private int currentQuestionIndex = 0;
    private int score = 0;

    public String getCurrentQuestion() {
        return questions[currentQuestionIndex];
    }

    public String getCurrentAnswer() {
        return answers[currentQuestionIndex];
    }

    public boolean checkAnswer(String userAnswer) {
        return userAnswer.equalsIgnoreCase(answers[currentQuestionIndex]);
    }

    public boolean isLastQuestion() {
        return currentQuestionIndex == questions.length - 1;
    }

    public void moveToNextQuestion() {
        currentQuestionIndex++;
    }

    public int getScore() {
        return score;
    }

    public void incrementScore() {
        score++;
    }
}