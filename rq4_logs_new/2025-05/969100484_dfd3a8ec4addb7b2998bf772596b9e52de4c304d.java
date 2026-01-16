package com.jia.study_tracker.service;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class StudyMessageFilterTest {

    private final StudyMessageFilter filter = new StudyMessageFilter();

    @Test
    void returnsTrueWhenTextContainsStudyKeywords() {
        assertTrue(filter.isStudyRelated("오늘 인강 2시간 들음"));
        assertTrue(filter.isStudyRelated("문제풀이를 했어요"));
        assertTrue(filter.isStudyRelated("강의 복습 중이에요"));
    }

    @Test
    void returnsFalseWhenTextDoesNotContainStudyKeywords() {
        assertFalse(filter.isStudyRelated("오늘 영화 봤어요"));
        assertFalse(filter.isStudyRelated("점심은 뭐 먹지"));
        assertFalse(filter.isStudyRelated("고양이가 귀엽다"));
    }

    @Test
    void returnsFalseWhenTextIsEmptyOrNull() {
        assertFalse(filter.isStudyRelated(""));
        assertFalse(filter.isStudyRelated(null));
    }
}