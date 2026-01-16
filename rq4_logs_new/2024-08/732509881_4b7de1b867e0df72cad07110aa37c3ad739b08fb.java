package com.example.demo.repositories;

import com.example.demo.models.CourseSession;
import com.example.demo.models.GlobalTableChange;
import org.springframework.data.jpa.repository.JpaRepository;

public interface GlobalTableChangeRepository extends JpaRepository<GlobalTableChange, Long> {
}