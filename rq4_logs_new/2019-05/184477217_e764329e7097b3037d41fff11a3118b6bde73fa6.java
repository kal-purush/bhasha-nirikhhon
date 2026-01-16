package com.triviamachine.triviamachine.controllers;
import com.triviamachine.triviamachine.database.*;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import static org.junit.Assert.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;


@RunWith(SpringRunner.class)
@SpringBootTest
public class ApplicaitonControllerTest {

        private MockMvc mockMvc;

        @Autowired
        public WebApplicationContext webApplicationContext;

        @Autowired
        AdminPanelController adminPanelController;

        @Autowired
        ApplicaitonController applicaitonController;

        @Autowired
        VoteController voteController;

        @Autowired
        AdminUserRepository adminUserRepository;
        //
        @Autowired
        QuestionRepository questionRepository;

        @Autowired
        QuestionScheduleRepository questionScheduleRepository;

        @Autowired
        ResultsRepository resultsRepository;

        @Before
        public void setUp() {
            mockMvc = MockMvcBuilders.webAppContextSetup(webApplicationContext).build();
        }


        @Test
    public void getLogin() throws Exception{
        mockMvc.perform(MockMvcRequestBuilders.get("/login"))
                .andExpect(status().isOk());
    }

//    @Test
//    public void getFrontPage() throws Exception{
//        mockMvc.perform(MockMvcRequestBuilders.get(""))
//                .andExpect(status().isOk());
//    }

    @Test
    public void getOlderResults() throws Exception {
        Question question = new Question();
        question.setQuestionText("idk");
        question.setAnswerOne("what");
        question.setAnswerTwo("huh");

        QuestionSchedule schedule = new QuestionSchedule();
        schedule.setQuestion(question);

        Results results = new Results();
        results.setQuestion(question);
        results.setAnswerOneVotes(4);
        results.setAnswerTwoVotes(5);

        questionRepository.save(question);

        questionScheduleRepository.save(schedule);

        resultsRepository.save(results);

        long id=question.getId();

        mockMvc.perform(MockMvcRequestBuilders.post("/admin/schedule/" + id + "?date=1222-01-21"))
                .andExpect(status().is(302));

        mockMvc.perform(MockMvcRequestBuilders.get("/results"))
                .andExpect(status().isOk())
                .andExpect(content().contentType("text/html;charset=UTF-8"))
                .andExpect(content().string(Matchers.containsString("idk")));

    }
}