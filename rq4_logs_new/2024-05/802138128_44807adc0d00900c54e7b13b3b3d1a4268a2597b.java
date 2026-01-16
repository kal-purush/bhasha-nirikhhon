package com.example.study;

import static org.junit.Assert.*;

import org.junit.Test;

public class QuestionTest {
    Question ques  = new Question(1, "Which organelle is responsible for cellular respiration?", "Mitochondria", "Chloroplasts", "Nucleus", "Endoplasmic reticulum", "Mitochondria");

    @Test
    public void getId() {
        assertEquals(1,ques.getId());
    }

    @Test
    public void setId() {
        ques.setId(2);
        assertEquals(2,ques.getId());
    }

    @Test
    public void getQuestion() {
        assertEquals("Which organelle is responsible for cellular respiration?",ques.getQuestion());
    }

    @Test
    public void getCorrectOption() {
        assertEquals("Mitochondria",ques.getCorrectOption());
    }

    @Test
    public void getOptionOne() {
        assertEquals("Mitochondria",ques.getOptionOne());
    }

    @Test
    public void getOptionTwo() {
        assertEquals("Chloroplasts",ques.getOptionTwo());
    }

    @Test
    public void getOptionThree() {
        assertEquals("Nucleus",ques.getOptionThree());
    }

    @Test
    public void getOptionFour() {
        assertEquals("Endoplasmic reticulum",ques.getOptionFour());
    }
}