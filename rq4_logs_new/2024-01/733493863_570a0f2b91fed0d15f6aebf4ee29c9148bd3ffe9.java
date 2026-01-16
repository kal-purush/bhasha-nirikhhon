package com.spring.jpa.PostgreDemo.PostgreDemo.DAO;

import com.spring.jpa.PostgreDemo.PostgreDemo.Entity.Student;
import java.util.List;
public interface StudentDAO {
    void save (Student theStudent);
    Student findById(Integer id);

    List<Student> findAll();

    void update(Student theStudent);

    void delete(Integer id);

    int deleteAll();
}