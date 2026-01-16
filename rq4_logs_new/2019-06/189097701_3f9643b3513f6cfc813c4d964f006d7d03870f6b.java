package ru.fmtk.hlystov.examinationapp.domain.examination.answer;

import org.junit.Test;

import static org.junit.Assert.*;

public class AnswerResultImplTest {

    @Test
    public void getDescriptionEn() {
        String descr = "Abcd 1234 @#%$^";
        AnswerResultImpl result = new AnswerResultImpl(descr, true);
        assertEquals(descr, result.getDescription());
    }

    @Test
    public void getDescriptionRu() {
        String descr = "ШэьёЙ 1234 @#%$^";
        AnswerResultImpl result = new AnswerResultImpl(descr, true);
        assertEquals(descr, result.getDescription());
    }

    @Test
    public void isRightTrue() {
        String descr = "Abcd 1234 @#%$^";
        AnswerResultImpl result = new AnswerResultImpl(descr, true);
        assertTrue(result.isRight());
    }

    @Test
    public void isRightFalse() {
        String descr = "Abcd 1234 @#%$^";
        AnswerResultImpl result = new AnswerResultImpl(descr, false);
        assertFalse(result.isRight());
    }
}