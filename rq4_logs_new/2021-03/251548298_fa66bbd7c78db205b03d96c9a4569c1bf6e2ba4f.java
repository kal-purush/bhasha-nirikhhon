package ca.bc.gov.educ.api.ruleengine.rule;

import ca.bc.gov.educ.api.ruleengine.struct.RuleData;
import ca.bc.gov.educ.api.ruleengine.struct.RuleProcessorData;
import ca.bc.gov.educ.api.ruleengine.struct.StudentCourse;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Data
@Component
@NoArgsConstructor
@AllArgsConstructor
public class CPCoursesRule implements Rule {

    private static Logger logger = LoggerFactory.getLogger(CPCoursesRule.class);

    @Autowired
    private RuleProcessorData ruleProcessorData;

    @Override
    public RuleData fire() {

        List<StudentCourse> studentCourseList = new ArrayList<StudentCourse>();
        studentCourseList = ruleProcessorData.getStudentCourses();

        logger.debug("###################### Finding CAREER PROGRAM courses ######################");

        for (StudentCourse studentCourse : studentCourseList) {
            if (studentCourse.getCourseCode().startsWith("CP")) {
                studentCourse.setCareerPrep(true);
            }
        }

        ruleProcessorData.setStudentCourses(studentCourseList);

        logger.info("Career Program Courses: " +
                (int) studentCourseList
                        .stream()
                        .filter(StudentCourse::isCareerPrep)
                        .count());

        return ruleProcessorData;
    }

    @Override
    public void setInputData(RuleData inputData) {
        ruleProcessorData = (RuleProcessorData) inputData;
        logger.info("CPCoursesRule: Rule Processor Data set.");
    }
}