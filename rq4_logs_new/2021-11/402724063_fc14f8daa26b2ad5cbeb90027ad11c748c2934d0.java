package seedu.academydirectory.logic.commands;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

import org.junit.jupiter.api.Test;

import seedu.academydirectory.model.student.Assessment;
import seedu.academydirectory.model.student.Student;
import seedu.academydirectory.testutil.StudentBuilder;
import seedu.academydirectory.testutil.TypicalStudents;

/**
 * Contains unit tests for VisualizeCommand.
 */
public class VisualizeCommandTest {

    @Test
    public void getOrderedAssessmentResults_validStudentList_returnLinkedHashMap() {
        List<Student> studentList = buildStudentListWithAssessment();

        VisualizeCommand visualizeCommand = new VisualizeCommand();
        LinkedHashMap<String, List<Integer>> actual = visualizeCommand.getOrderedAssessmentResults(studentList);

        LinkedHashMap<String, List<Integer>> expected = new LinkedHashMap<>();
        expected.put("RA1", Arrays.asList(1, 2));
        expected.put("MIDTERM", Arrays.asList(2, 3));
        expected.put("RA2", Arrays.asList(3, 4));
        expected.put("PE", Arrays.asList(4, 5));
        expected.put("FINAL", Arrays.asList(5, 6));

        assertEquals(actual, expected);
    }

    @Test
    public void assessmentCorrectOrder_validStudentList_sameOrderAsAssessmentClass() {
        List<Student> studentList = buildStudentListWithAssessment();
        VisualizeCommand visualizeCommand = new VisualizeCommand();
        LinkedHashMap<String, List<Integer>> assessmentMap = visualizeCommand.getOrderedAssessmentResults(studentList);

        ArrayList<String> assessmentsKey = new ArrayList<>(assessmentMap.keySet());

        assertEquals(assessmentsKey, Assessment.ASSESSMENT_LIST);
    }

    private static List<Student> buildStudentListWithAssessment() {
        Student Alice = new StudentBuilder(TypicalStudents.ALICE)
                .withAssessment(getAssessment(1, 2, 3, 4, 5))
                .build();

        Student Bob = new StudentBuilder(TypicalStudents.BOB)
                .withAssessment(getAssessment(2, 3, 4, 5, 6))
                .build();

        List<Student> studentList = new ArrayList<>();
        studentList.add(Alice);
        studentList.add(Bob);

        return studentList;
    }

    private static HashMap<String, Integer> getAssessment(Integer... gradeArray) {
        assert gradeArray.length == Assessment.ASSESSMENT_LIST.size()
                : "Provided list of grade has different length from list of supported Assessment";
        Assessment assessment = new Assessment();

        for (int i = 0; i < gradeArray.length; i += 1) {
            assessment.updateAssessmentGrade(Assessment.ASSESSMENT_LIST.get(i),
                    gradeArray[i]);
        }

        return assessment.getCopy().getAssessmentHashMap();
    }
}