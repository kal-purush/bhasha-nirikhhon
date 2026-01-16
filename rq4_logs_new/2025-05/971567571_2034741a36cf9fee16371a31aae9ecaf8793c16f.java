package ru.hogwarts.school.controller;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import ru.hogwarts.school.model.Faculty;
import ru.hogwarts.school.service.FacultyService;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(MockitoExtension.class)
class FacultyControllerMockTest {

    private MockMvc mockMvc;

    @Mock
    private FacultyService facultyService;

    @InjectMocks
    private FacultyController facultyController;

    private final Faculty testFaculty = new Faculty(1L, "Gryffindor", "Red");

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders.standaloneSetup(facultyController).build();
    }

    @Test
    void createFaculty_shouldReturnFacultyAndStatusOk() throws Exception {
        when(facultyService.createFaculty(any(Faculty.class))).thenReturn(testFaculty);

        mockMvc.perform(post("/faculty")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"Gryffindor\",\"color\":\"Red\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(1))
                .andExpect(jsonPath("$.name").value("Gryffindor"));
    }

    @Test
    void getFacultyById_shouldReturnFacultyWhenExists() throws Exception {
        when(facultyService.getFacultyById(anyLong())).thenReturn(Optional.of(testFaculty));

        mockMvc.perform(get("/faculty/1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.color").value("Red"));
    }

    @Test
    void getFacultyById_shouldReturnNotFoundWhenNotExists() throws Exception {
        when(facultyService.getFacultyById(anyLong())).thenReturn(Optional.empty());

        mockMvc.perform(get("/faculty/999"))
                .andExpect(status().isNotFound());
    }

    @Test
    void updateFaculty_shouldReturnUpdatedFaculty() throws Exception {
        Faculty updated = new Faculty(1L, "Gryffindor Updated", "Scarlet");
        when(facultyService.updateFaculty(any(Faculty.class))).thenReturn(updated);

        mockMvc.perform(put("/faculty")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"id\":1,\"name\":\"Gryffindor Updated\",\"color\":\"Scarlet\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.color").value("Scarlet"));
    }

    @Test
    void deleteFaculty_shouldReturnStatusOk() throws Exception {
        mockMvc.perform(delete("/faculty/1"))
                .andExpect(status().isOk());
    }

    @Test
    void getFacultiesByColor_shouldReturnFacultiesList() throws Exception {
        when(facultyService.getFacultiesByColor(anyString())).thenReturn(Collections.singletonList(testFaculty));

        mockMvc.perform(get("/faculty/color/Red"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].name").value("Gryffindor"));
    }

    @Test
    void searchFaculties_shouldReturnMatchingFaculties() throws Exception {
        when(facultyService.searchFaculties(anyString())).thenReturn(List.of(testFaculty));

        mockMvc.perform(get("/faculty/search")
                        .param("searchTerm", "Gryff"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].id").value(1));
    }

    @Test
    void getFacultyStudents_shouldReturnStudentsList() throws Exception {
        mockMvc.perform(get("/faculty/1/students"))
                .andExpect(status().isOk());
    }
}