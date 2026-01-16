package tests.TopDown;

import domain.Student;
import repository.StudentXMLRepo;

public class StudentXMLRepoMock extends StudentXMLRepo {
    /**
     * Class constructor
     *
     * @param filename - numele fisierului
     */
    private Student student;

    public StudentXMLRepoMock(String filename) {
        super(filename);
    }

    @Override
    public Student findOne(String s) {
        return student;
    }

    @Override
    public Student save(Student entity) {
        student = entity;
        return null;
    }
}