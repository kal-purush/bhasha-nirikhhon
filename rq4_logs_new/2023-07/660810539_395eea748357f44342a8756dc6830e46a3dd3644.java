package com.practice.studentenrollment.SearchApi.Repository;

import com.practice.studentenrollment.SearchApi.Model.StudentSearch;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface StudentSearchRepository extends JpaRepository<StudentSearch, Integer> {
    List<StudentSearch> findByFirstNameIgnoreCase(String firstName);
    List<StudentSearch> findByLastNameIgnoreCase(String lastName);
    List<StudentSearch> findByUserNameIgnoreCase(String userName);

    @Query("SELECT s FROM StudentSearch s WHERE UPPER(s.name) LIKE CONCAT('%', UPPER(:name), '%')")
    List<StudentSearch> findByNameIgnoreCase(String name);
}