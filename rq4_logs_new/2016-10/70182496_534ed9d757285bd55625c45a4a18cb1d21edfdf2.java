package com.courses.db.dao.testNG;


import com.courses.db.dao.MarkDao;
import com.courses.db.dao.StudentDao;
import com.courses.db.dao.SubjectDao;
import com.courses.db.dto.Mark;
import com.courses.db.dto.Student;
import com.courses.db.dto.Subject;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

public class MarkDaoTestNG {

    private static MarkDao markDao;
    private static SubjectDao subjectDao;
    private static StudentDao studentDao;

    @BeforeClass
    public static void beforeClass() {
        markDao = new MarkDao();
        studentDao = new StudentDao();
        subjectDao = new SubjectDao();
    }

    @AfterClass
    public static void afterClass() {
        markDao.close();
        studentDao.close();
        subjectDao.close();
    }

    @BeforeMethod
    public void setUp() throws Exception {
        studentDao.insertStudent(new Student("Test_Name_1", "Test_Name_2"));
        Student student = studentDao.getAll().stream().filter(x -> "Test_Name_1".equals(x.getFirstName())).findFirst().get();
        int id2 = student.getId();
        subjectDao.insertSubject(new Subject("Test_1"));
        Subject subject = subjectDao.getAllSudject().stream().filter(x -> "Test_1".equals(x.getName())).findFirst().get();
        int id3 = subject.getId();
        markDao.insertMark(new Mark(id2, id3, 8));

    }

    @AfterMethod
    public void tearDown() throws Exception {
        List<Mark> marks = markDao.getAllMark();
        for (Mark m : marks) {
            markDao.delateMark(m.getId());
        }
        List<Subject> subjects = subjectDao.getAllSudject();
        for (Subject s : subjects) {
            subjectDao.delateSubject(s.getId());
        }
        List<Student> students = studentDao.getAll();
        for (Student s : students) {
            studentDao.delateStudent(s.getId());
        }
    }

    @Test
    public void testGetMarktById() throws Exception {
        Mark mark = markDao.getAllMark().stream().filter(x -> (Integer.valueOf(8)).equals(x.getMark())).findFirst().get();
        int id1 = mark.getId();
        Student student = studentDao.getAll().stream().filter(x -> "Test_Name_1".equals(x.getFirstName())).findFirst().get();
        int id2 = student.getId();
        Subject subject = subjectDao.getAllSudject().stream().filter(x -> "Test_1".equals(x.getName())).findFirst().get();
        int id3 = subject.getId();

        Assert.assertEquals(id2, markDao.getMarktById(id1).getStudent_id());
        Assert.assertEquals(id3, markDao.getMarktById(id1).getSubject_id());
        Assert.assertEquals(8, markDao.getMarktById(id1).getMark());


    }

    @Test
    public void testGetAllMark() throws Exception {
        List<Mark> marks = markDao.getAllMark();
        Assert.assertEquals(1, marks.size());

    }

    @Test
    public void testUpdateMark() throws Exception {
        Mark mark = markDao.getAllMark().stream().filter(x -> (Integer.valueOf(8)).equals(x.getMark())).findFirst().get();
        int id1 = mark.getId();
        Student student = studentDao.getAll().stream().filter(x -> "Test_Name_1".equals(x.getFirstName())).findFirst().get();
        int id2 = student.getId();
        Subject subject = subjectDao.getAllSudject().stream().filter(x -> "Test_1".equals(x.getName())).findFirst().get();
        int id3 = subject.getId();
        mark = new Mark(id2, id3, 4);
        markDao.updateMark(mark, id1);
        Assert.assertEquals(4, markDao.getMarktById(id1).getMark());

    }

}