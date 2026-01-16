package com.gv.app.services.impls;

import com.gv.app.models.Student;
import com.gv.app.services.interfaces.StudentService;
import org.junit.Assert;
import org.junit.Test;

public class StudentServiceHibernateTest {

    private StudentService service = new StudentServiceHibernate();

    @Test
    public void addStudentTest() throws Exception {
        Student student = new Student("XXX", "YYY", "xy@gmail.com");
        String errorMessage = "Adding of element was not successful.";
        Assert.assertTrue(errorMessage, service.addStudent(student));
    }

    @Test
    public void addNullTest() throws Exception {
        Student student = null;
        String errorMessage = "Adding of null reference was successful.";
        Assert.assertFalse(errorMessage, service.addStudent(student));
    }

    @Test
    public void getExistedStudentTest() throws Exception {
        int id = 4;
        String errorMessage = "There is no student with such id.";
        Assert.assertEquals(errorMessage, service.getStudent(id).getFirstName(), "XXX");
    }

    @Test
    public void updateStudentTest() throws Exception {
        int id = 5;
        Student student = new Student("ZZZ", "YYY", "zy@gmail.com");
        String errorMessage = "Actual and expected names are not correspond.";
        Assert.assertEquals(errorMessage, service.updateStudent(id, student), 1);
    }

    @Test
    public void deleteStudentTest() throws Exception {
        int id = 6;
        String errorMessage = "Deleting was not successful";
        Assert.assertTrue(errorMessage, service.deleteStudent(id));
    }

    @Test
    public void deleteNotExistedStudentTest() throws Exception {
        int id = 6;
        String errorMessage = "Deleting was not successful";
        Assert.assertTrue(errorMessage, service.deleteStudent(id));
    }

}