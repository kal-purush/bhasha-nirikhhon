package spring.dao;

import org.junit.jupiter.api.Test;
import otus.spring.config.TestFileNameProvider;
import otus.spring.dao.CsvQuestionDao;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


class CsvQuestionDaoTest {

    @Test
    void findAll() {
        var fileNameProvider = mock(TestFileNameProvider.class);
        when(fileNameProvider.getTestFileName()).thenReturn("questionsTest.csv");
        var csvQuestionDao = new CsvQuestionDao(fileNameProvider);
        assertEquals(3, csvQuestionDao.findAll().size(), "expected 3 returned returned another number");
    }
}