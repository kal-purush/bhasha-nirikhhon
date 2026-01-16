package ch.uzh.ifi.hase.soprafs23.service;

import ch.uzh.ifi.hase.soprafs23.constant.ApiUrls;
import ch.uzh.ifi.hase.soprafs23.constant.UserStatus;
import ch.uzh.ifi.hase.soprafs23.entity.User;
import ch.uzh.ifi.hase.soprafs23.game.stateStorage.Question;
import ch.uzh.ifi.hase.soprafs23.repository.UserRepository;
import ch.uzh.ifi.hase.soprafs23.rest.dto.TopicGetDTO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

public class ApiServiceUnitTests {

    @InjectMocks
    private ApiService apiService;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);

    }

    @Test
    public void testGetTopics_ReturnsListWithContent() throws Exception {
        List<TopicGetDTO> topics = apiService.getTopicsFromApi("https://opentdb.com/api_category.php");
        assertTrue(topics.size()>0);
    }

    @Test
    public void testGetQuestions_ReturnsListWithContent() throws Exception {
        //String.format(ApiUrls.QUESTIONS.url, gameRoom.getNumOfQuestions(), gameRoom.getQuestionTopicId(), gameRoom.getGameMode());
        String url = String.format(ApiUrls.QUESTIONS.url,3,22,"medium");
        List<Question> questions = apiService.getQuestionsFromApi(url);
        assertTrue(questions.size()>0);
    }
}