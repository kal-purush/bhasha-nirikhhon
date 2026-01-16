package uk.ac.bangor.cs.cambria.AcademiGymraeg.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

import uk.ac.bangor.cs.cambria.AcademiGymraeg.TestConfigurer;
import uk.ac.bangor.cs.cambria.AcademiGymraeg.model.Test;
import uk.ac.bangor.cs.cambria.AcademiGymraeg.repo.TestRepository;

@Controller
public class TestController {

    @Autowired
    private TestRepository testRepo;

    @Autowired
    private TestConfigurer testConfig;

    @GetMapping("/test")
    public String takeTest() {

        Test test = new Test();

        testConfig.generateQuestionsForTest(test);

        testRepo.save(test);

        return "taketest"; // Return the name of the HTML template to render
    }

    
}
//2.) GET Handler
/* - Create a Test Object
- Creating Test Model
- Using TestRepo to Save to DB
- Getting TestId
- Create Questions using QuestionConfigurer for the tests
- Render HTML with the Questions, using appropriate input types */