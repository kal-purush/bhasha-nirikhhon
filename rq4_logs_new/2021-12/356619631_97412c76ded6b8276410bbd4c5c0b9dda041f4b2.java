package org.uresti.pozarreal.service.impl;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.uresti.pozarreal.dto.Course;
import org.uresti.pozarreal.dto.CourseOccurrence;
import org.uresti.pozarreal.model.ScheduleByCourse;
import org.uresti.pozarreal.repository.CourseRepository;
import org.uresti.pozarreal.repository.ScheduleByCourseRepository;

import java.util.LinkedList;
import java.util.List;

public class CoursesServiceImplTests {

    @Test
    public void givenAnEmptyCoursesList_whenFindAll_thenEmptyListIsReturned() {
        // Given:
        CourseRepository courseRepository = Mockito.mock(CourseRepository.class);
        ScheduleByCourseRepository scheduleByCourseRepository = Mockito.mock(ScheduleByCourseRepository.class);

        CoursesServiceImpl coursesService = new CoursesServiceImpl(courseRepository, scheduleByCourseRepository);

        List<org.uresti.pozarreal.model.Course> courses = new LinkedList<>();

        Mockito.when(courseRepository.findAll()).thenReturn(courses);

        // When:
        List<Course> courseList = coursesService.findAll();

        // Then:
        Assertions.assertThat(courseList.isEmpty()).isTrue();
    }

    @Test
    public void givenAnCoursesListWithTwoElements_whenFindAll_thenListWithTwoElementsIsReturned() {
        // Given:
        CourseRepository courseRepository = Mockito.mock(CourseRepository.class);
        ScheduleByCourseRepository scheduleByCourseRepository = Mockito.mock(ScheduleByCourseRepository.class);

        CoursesServiceImpl coursesService = new CoursesServiceImpl(courseRepository, scheduleByCourseRepository);

        List<Course> courses = new LinkedList<>();

        Course course1 = Course.builder()
                .name("Name 1")
                .id("id1")
                .professorId("profe1")
                .professorName("Profe name 1")
                .build();

        Course course2 = Course.builder()
                .name("Name 2")
                .id("id2")
                .professorId("profe2")
                .professorName("Profe name 2")
                .build();

        courses.add(course1);
        courses.add(course2);

        Mockito.when(courseRepository.getCoursesInfo()).thenReturn(courses);

        // When:
        List<Course> courseList = coursesService.findAll();

        // Then:
        Assertions.assertThat(courseList.size()).isEqualTo(2);
        Assertions.assertThat(courseList.get(0).getName()).isEqualTo("Name 1");
        Assertions.assertThat(courseList.get(0).getId()).isEqualTo("id1");
        Assertions.assertThat(courseList.get(0).getProfessorId()).isEqualTo("profe1");
        Assertions.assertThat(courseList.get(0).getProfessorName()).isEqualTo("Profe name 1");
        Assertions.assertThat(courseList.get(1).getName()).isEqualTo("Name 2");
        Assertions.assertThat(courseList.get(1).getId()).isEqualTo("id2");
        Assertions.assertThat(courseList.get(1).getProfessorId()).isEqualTo("profe2");
        Assertions.assertThat(courseList.get(1).getProfessorName()).isEqualTo("Profe name 2");
    }

    @Test
    public void givenScheduleByCourse_whenConvertToOccurrenceDto_thenConvertToOccurrenceDto() {
        // Given:
        CourseRepository courseRepository = Mockito.mock(CourseRepository.class);
        ScheduleByCourseRepository scheduleByCourseRepository = Mockito.mock(ScheduleByCourseRepository.class);

        CoursesServiceImpl coursesService = new CoursesServiceImpl(courseRepository, scheduleByCourseRepository);

        List<Course> courses = new LinkedList<>();
        List<CourseOccurrence> courseOccurrences = new LinkedList<>();
        List<ScheduleByCourse> scheduleByCourses = new LinkedList<>();

        CourseOccurrence courseOccurrence = CourseOccurrence.builder()
                .startTime("10:00")
                .endTime("12:00")
                .label("Wednesday")
                .id("id1")
                .build();

        courseOccurrences.add(courseOccurrence);

        Course course1 = Course.builder()
                .name("Name 1")
                .id("id1")
                .professorId("profe1")
                .professorName("Profe name 1")
                .professorPhone("123456")
                .professorAddress("Address")
                .occurrences(courseOccurrences)
                .build();

        courses.add(course1);

        ScheduleByCourse scheduleByCourse = ScheduleByCourse.builder()
                .courseId("id1")
                .endTime(1200)
                .startTime(1000)
                .weekday("Wednesday")
                .id("id1")
                .build();

        scheduleByCourses.add(scheduleByCourse);

        Mockito.when(courseRepository.getCoursesInfo()).thenReturn(courses);
        Mockito.when(scheduleByCourseRepository.findAllByCourseId("id1")).thenReturn(scheduleByCourses);

        // When:
        List<Course> courseList = coursesService.findAll();

        // Then:
        Assertions.assertThat(courseList.size()).isEqualTo(1);
        Assertions.assertThat(courseList.get(0).getName()).isEqualTo("Name 1");
        Assertions.assertThat(courseList.get(0).getId()).isEqualTo("id1");
        Assertions.assertThat(courseList.get(0).getProfessorId()).isEqualTo("profe1");
        Assertions.assertThat(courseList.get(0).getProfessorName()).isEqualTo("Profe name 1");
        Assertions.assertThat(courseList.get(0).getOccurrences().get(0).getId()).isEqualTo(courseOccurrence.getId());
        Assertions.assertThat(courseList.get(0).getOccurrences().get(0).getStartTime()).isEqualTo(courseOccurrence.getStartTime());
        Assertions.assertThat(courseList.get(0).getOccurrences().get(0).getEndTime()).isEqualTo(courseOccurrence.getEndTime());
        Assertions.assertThat(courseList.get(0).getOccurrences().get(0).getLabel()).isEqualTo(courseOccurrence.getLabel());
        Assertions.assertThat(courseList.get(0).getOccurrences().get(0).getStartTime()).isEqualTo("10:00");
        Assertions.assertThat(courseList.get(0).getOccurrences().get(0).getEndTime()).isEqualTo("12:00");
    }
}