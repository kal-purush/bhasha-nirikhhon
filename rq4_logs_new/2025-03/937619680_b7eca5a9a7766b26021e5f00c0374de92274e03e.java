package com.gowri.blitz.service.impl;

import com.gowri.blitz.modal.Student;
import com.gowri.blitz.repo.StudentRepo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Date;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class StudentImplTest {

    @Mock
    private StudentRepo studentRepo;

    @InjectMocks
    private StudentImpl studentService;

    private Student student;

    @BeforeEach
    void setUp() {
        // Initialize the student object before each test
        student = new Student();
        student.setStId(1);
        student.setFirstName("John Doe");
        student.setDateOfBirth(new Date());
    }

    @Test
    void testAddStudent() {
        // Arrange: Mock the repository save method
        when(studentRepo.save(any(Student.class))).thenReturn(student);

        // Act: Call the addStudent method
        Student result = studentService.addStudent(student);

        // Assert: Verify that the result matches the expected student object
        assertNotNull(result);
        assertEquals(student.getStId(), result.getStId());
        assertEquals(student.getFirstName(), result.getFirstName());
        verify(studentRepo, times(1)).save(any(Student.class));
    }

    @Test
    void testFindStudent() {
        // Arrange: Mock the repository findById method
        when(studentRepo.findById(1)).thenReturn(Optional.of(student));

        // Act: Call the findStudent method
        Student result = studentService.findStudent(1);

        // Assert: Verify the student is returned correctly
        assertNotNull(result);
        assertEquals(student.getStId(), result.getStId());
        assertEquals(student.getFirstName(), result.getFirstName());
        verify(studentRepo, times(1)).findById(1);
    }

    @Test
    void testDeleteStudent() {
        // Arrange: Mock the repository deleteById method
        doNothing().when(studentRepo).deleteById(1);

        // Act: Call the deleteStudent method
        studentService.deleteStudent(1);

        // Assert: Verify that deleteById method was called once
        verify(studentRepo, times(1)).deleteById(1);
    }

    @Test
    void testEditStudent() {
        // Act: Call the editStudent method (currently does nothing in the implementation)
        Student result = studentService.editStudent(1);

        // Assert: Verify the result is null since editStudent is not implemented
        assertNull(result);
    }
}