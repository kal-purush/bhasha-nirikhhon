package com.example.demo.repository;

import com.example.demo.course.entity.CourseLandmark;
import com.example.demo.course.repository.CourseLandmarkRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;

@SpringBootTest
public class CourseLandmarkRepositoryTest {

    @Autowired
    CourseLandmarkRepository repository;

    @Test
    public void 리스트검색(){
        List<CourseLandmark> list = repository.readByUserId("11");
        for (CourseLandmark landmark : list){
            System.out.println(landmark);
        }
    }
}