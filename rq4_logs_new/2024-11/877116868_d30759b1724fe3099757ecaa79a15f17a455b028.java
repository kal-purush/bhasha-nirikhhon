package com.eng;

import com.eng.domain.*;
import com.eng.repository.*;
import com.eng.service.RedisService;
import com.eng.service.StudyService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;



// Junit5 기능을 사용하고, Test에서 가짜 객체를 사용하기 때문에 @ExtendWith(SpringExtension.class)를 붙여줘야 한다.
@ExtendWith(SpringExtension.class)
public class QuizServiceTest {
    // Test 주체
    StudyService studyService;
    // Test 협력자
    @MockBean
    JdbcRepository jdbcRepository;
    @MockBean
    QuizRepository quizRepository;
    @MockBean
    UserRepository userRepository;
    @MockBean
    MeanRepository meanRepository;
    @MockBean
    StudyRepository studyRepository;
    @MockBean
    SentenceRepository sentenceRepository;
    @MockBean
    RedisService redisService;
    @MockBean
    WordRepository wordRepository;

    @BeforeEach
    void setUp() {
        studyService = new StudyService(
                userRepository, meanRepository,studyRepository,
                sentenceRepository, redisService, wordRepository,
                jdbcRepository, quizRepository
        );
    }

    private List<Quiz> getQuizList() {
        User user = new User();
        Word word = Word.builder().build();
        Meaning meaning = Meaning.builder().word(word).meaning("meaning").build();
        Sentence sentence = Sentence.builder().meaning(meaning).sentence("sentence").sentence_meaning("sentence_meaning").level(0).build();
        LocalDate date = LocalDate.now();
        List<Study> studies = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            studies.add(Study.createStudy(user, word, meaning, sentence, date));
        }
        return studies.stream()
                .map(Quiz::addQuiz)
                .toList();
    }

    @Test
    @DisplayName("Batch Insert")
    public void test() {
        List<Quiz> quizList = getQuizList();
        long start = System.nanoTime();
        jdbcRepository.batchInsert(quizList);
        long end = System.nanoTime();
        System.out.print("end-start : " + (end-start));
    }

    @Test
    @DisplayName("SAVE ALL")
    public void saveAll() {
        List<Quiz> quizList = getQuizList();
        long start = System.nanoTime();
        quizRepository.saveAll(quizList);
        long end = System.nanoTime();
        System.out.print("end-start : " + (end-start));
    }
}