package com.horapp.service;

import com.horapp.persistence.entity.Schedule;
import com.horapp.persistence.repository.ScheduleRepository;
import com.horapp.presentation.dto.request.ScheduleRequestDTO;
import com.horapp.presentation.dto.response.ScheduleResponseDTO;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.jdbc.Sql;
import org.webjars.NotFoundException;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@ActiveProfiles("test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Sql(scripts = {"/db_templates_test/ScheduleUseCaseTestInserts.sql"}, executionPhase = Sql.ExecutionPhase.BEFORE_TEST_CLASS)
@Sql(scripts = {"/db_templates_test/DeleteAllTemplateSqlTest.sql"}, executionPhase = Sql.ExecutionPhase.AFTER_TEST_CLASS)
public class ScheduleServiceTest {

    @Autowired
    private ScheduleService scheduleService;

    @Autowired
    private ScheduleRepository scheduleRepository;

    //---------------------- INTEGRATION TESTS ------------------------------------
    @Test
    public void saveTest_successful(){
        Long idCourseExpected = 12000L;
        Long idSchedule = 150000L;
        String courseGroupExpected = "3K1";
        ScheduleRequestDTO scheduleRequestDTO = new ScheduleRequestDTO("3K1", 12000L);
        ScheduleResponseDTO scheduleResponseDTO = assertDoesNotThrow(() -> scheduleService.save(scheduleRequestDTO));
        Schedule scheduleSaved = scheduleRepository.findById(idSchedule).get();
        assertEquals(idCourseExpected, scheduleSaved.getCourse().getIdCourse());
        assertEquals(courseGroupExpected, scheduleSaved.getCourseGroup());
    }

    @Test
    public void saveTest_WhenDoesNotExistCourse(){
        Long idCourse = 1L; // Course with id 1 does not exist
        ScheduleRequestDTO scheduleRequestDTO = new ScheduleRequestDTO("3K1", idCourse);
        assertThrows(DataIntegrityViolationException.class, () -> scheduleService.save(scheduleRequestDTO));
    }

    @Test
    public void findByIdTest_successful(){
        Long idSchedule = 140000L;
        String courseGroupExpected = "1K5";
        String dayExpected = "MONDAY";
        ScheduleResponseDTO response = assertDoesNotThrow(() -> scheduleService.findById(idSchedule));
        assertEquals(idSchedule, response.idSchedule());
        assertEquals(courseGroupExpected, response.courseGroup());
        response.days().forEach(day -> assertEquals(dayExpected, day));
    }

    @Test
    public void findByIdTest_WhenDoesNotExists(){
        Long idSchedule = 1L; // Schedule with id 1 does not exist
        assertThrows(NotFoundException.class, () -> scheduleService.findById(idSchedule));
    }


    // ------------------------- UNIT TEST ---------------------------

}