package se.kth.sda.skeleton.assignments;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import se.kth.sda.skeleton.student.Student;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.mockito.Mockito.when;

@SpringBootTest
public class AssignmentControllerTest {


    @Autowired
    AssignmentController assController;

    @MockBean
    AssignmentService assService;

    @Test
    public void should_ReturnThreeAssignments_WhenViewAll() {

        // arrange
        Assignment assignment1 = new Assignment(1L, "ass1", "assDes1", "Q1", "Q2", "Q3","Q4","Q5","Q6","Q7","Q8","Q9","Q10");
        Assignment assignment2 = new Assignment(2L, "ass2", "assDes2", "Q2.1", "Q2.2", "Q2.3","Q2.4","Q2.5","Q2.6","Q2.7","Q2.8","Q2.9","Q2.10");
        Assignment assignment3 = new Assignment(3L, "ass3", "assDes3", "Q3.1", "Q3.2", "Q3.3","Q3.4","Q3.5","Q3.6","Q3.7","Q3.8","Q3.9","Q3.10");


        List<Assignment> threeAssignments = new ArrayList<>();
        threeAssignments.add(assignment1);
        threeAssignments.add(assignment2);
        threeAssignments.add(assignment3);
        when(assService.viewAll())
                .thenReturn(threeAssignments);


        //act
        List<Assignment> actualAssignments = assController.viewAll();

        //assert
        Assertions.assertEquals(threeAssignments.size(),actualAssignments.size());
        Assertions.assertEquals(threeAssignments.get(0).getAssignmentTitle(),threeAssignments.get(0).getAssignmentTitle());
        Assertions.assertEquals(threeAssignments.get(1).getAssignmentTitle(),threeAssignments.get(1).getAssignmentTitle());
    }


    @Test
    void should_ReturnOneStudent_WhenGetById() {


        //arrange
        Assignment assignment1 = new Assignment(1L, "ass1", "assDes1", "Q1", "Q2", "Q3","Q4","Q5","Q6","Q7","Q8","Q9","Q10");
        Assignment assignment2 = new Assignment(2L, "ass2", "assDes2", "Q2.1", "Q2.2", "Q2.3","Q2.4","Q2.5","Q2.6","Q2.7","Q2.8","Q2.9","Q2.10");
        Assignment assignment3 = new Assignment(3L, "ass3", "assDes3", "Q3.1", "Q3.2", "Q3.3","Q3.4","Q3.5","Q3.6","Q3.7","Q3.8","Q3.9","Q3.10");


        when(assService.getById(1L))
                .thenReturn(Optional.of(assignment1));
        when(assService.getById(2L))
                .thenReturn(Optional.of(assignment2));
        when(assService.getById(3L))
                .thenReturn(Optional.of(assignment3));

        //act
        Assignment controllerAssignment1 = assController.getById(1L);
        Assignment controllerAssignment2 = assController.getById(2L);

        //assert
        Assertions.assertEquals(assignment1.getAssignmentTitle(), controllerAssignment1.getAssignmentTitle());
        Assertions.assertEquals(assignment2.getAssignmentTitle(), controllerAssignment2.getAssignmentTitle());

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