package se.kth.sda.skeleton.answeredAssignments;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.mockito.Mockito.when;

@SpringBootTest
public class AnsweredAssignmentControllerTest {

    @Autowired
    AnsweredAssignmentController answeredAssController;

    @MockBean
    AnsweredAssignmentService answeredAssService;

    @Test
    public void should_ReturnThreeAssignments_WhenViewAll() {

        // arrange
        AnsweredAssignment answeredAssignment1 = new AnsweredAssignment(1L, "ansAss1", "A1", "A2", "A3","A4","A5","A6","A7","A8","A9","A10",1L);
        AnsweredAssignment answeredAssignment2 = new AnsweredAssignment(2L, "ansAss2", "A2.1", "A2.2", "A2.3","A2.4","A2.5","A2.6","A2.7","A2.8","A2.9","A2.10", 2L);
        AnsweredAssignment answeredAssignment3 = new AnsweredAssignment(3L, "ansAss3",  "A3.1", "A3.2", "A3.3","A3.4","A3.5","A3.6","A3.7","A3.8","A3.9","A3.10", 3L);


        List<AnsweredAssignment> threeAnsweredAssignments = new ArrayList<>();
        threeAnsweredAssignments.add(answeredAssignment1);
        threeAnsweredAssignments.add(answeredAssignment2);
        threeAnsweredAssignments.add(answeredAssignment3);
        when(answeredAssService.viewAll())
                .thenReturn(threeAnsweredAssignments);


        //act
        List<AnsweredAssignment> actualAssignments = answeredAssController.viewAll();

        //assert
        Assertions.assertEquals(threeAnsweredAssignments.size(),actualAssignments.size());
        Assertions.assertEquals(threeAnsweredAssignments.get(0).getAnsweredAssignmentTitle(),threeAnsweredAssignments.get(0).getAnsweredAssignmentTitle());
        Assertions.assertEquals(threeAnsweredAssignments.get(1).getAnsweredAssignmentTitle(),threeAnsweredAssignments.get(1).getAnsweredAssignmentTitle());
    }


    @Test
    void should_ReturnOneStudent_WhenGetById() {


        //arrange
        AnsweredAssignment answeredAssignment1 = new AnsweredAssignment(1L, "ansAss1", "A1", "A2", "A3","A4","A5","A6","A7","A8","A9","A10",1L);
        AnsweredAssignment answeredAssignment2 = new AnsweredAssignment(2L, "ansAss2", "A2.1", "A2.2", "A2.3","A2.4","A2.5","A2.6","A2.7","A2.8","A2.9","A2.10", 2L);
        AnsweredAssignment answeredAssignment3 = new AnsweredAssignment(3L, "ansAss3",  "A3.1", "A3.2", "A3.3","A3.4","A3.5","A3.6","A3.7","A3.8","A3.9","A3.10", 3L);


        when(answeredAssService.getById(1L))
                .thenReturn(Optional.of(answeredAssignment1));
        when(answeredAssService.getById(2L))
                .thenReturn(Optional.of(answeredAssignment2));
        when(answeredAssService.getById(3L))
                .thenReturn(Optional.of(answeredAssignment3));

        //act
        AnsweredAssignment controllerAnsweredAssignment1 = answeredAssController.getById(1L);
        AnsweredAssignment controllerAnsweredAssignment2 = answeredAssController.getById(2L);

        //assert
        Assertions.assertEquals(answeredAssignment1.getAnsweredAssignmentTitle(), controllerAnsweredAssignment1.getAnsweredAssignmentTitle());
        Assertions.assertEquals(answeredAssignment2.getAnsweredAssignmentTitle(), controllerAnsweredAssignment2.getAnsweredAssignmentTitle());

    }

    @Test
    void testCreate() {
    }

    @Test
    void testUpdate() {
    }

    @Test
    void testDelete() {
    }
}