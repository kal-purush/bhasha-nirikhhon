package org.uresti.pozarreal.service.impl;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.uresti.pozarreal.exception.BadRequestDataException;
import org.uresti.pozarreal.model.CourseAssistant;
import org.uresti.pozarreal.repository.*;

import java.time.LocalDate;
import java.util.LinkedList;
import java.util.List;

public class CourseAssistantServiceImplTests {

    @Test
    public void givenAnEmptyList_whenFindAllByCourse_thenEmptyListIsReturned() {
        // Given:
        CourseAssistantRepository courseAssistantRepository = Mockito.mock(CourseAssistantRepository.class);

        CourseAssistantServiceImpl courseAssistantService = new CourseAssistantServiceImpl(courseAssistantRepository);

        List<CourseAssistant> courseAssistants = new LinkedList<>();

        Mockito.when(courseAssistantRepository.findAllByCourseId("")).thenReturn(courseAssistants);

        // When:
        List<org.uresti.pozarreal.dto.CourseAssistant> courseAssistantList = courseAssistantService.findAllByCourse("");

        // Then:
        Assertions.assertThat(courseAssistantList.isEmpty()).isTrue();
    }

    @Test
    public void givenAnECourseAssistantWithOneElement_whenFindAllByCourse_thenListWithOneElementIsReturned() {
        // Given:
        CourseAssistantRepository courseAssistantRepository = Mockito.mock(CourseAssistantRepository.class);

        CourseAssistantServiceImpl courseAssistantService = new CourseAssistantServiceImpl(courseAssistantRepository);

        List<CourseAssistant> courseAssistants = new LinkedList<>();

        LocalDate now = LocalDate.now();

        CourseAssistant courseAssistant = CourseAssistant.builder()
                .id("id1")
                .courseId("course1")
                .notes("hello")
                .email("email@email.com")
                .birthDate(now)
                .phone("123456789")
                .name("name")
                .responsibleName("name1")
                .responsibleName2("name2")
                .responsiblePhone("phone1")
                .responsiblePhone2("phone2")
                .build();

        courseAssistants.add(courseAssistant);

        Mockito.when(courseAssistantRepository.findAllByCourseId("course1")).thenReturn(courseAssistants);

        // When:
        List<org.uresti.pozarreal.dto.CourseAssistant> courseAssistantList = courseAssistantService.findAllByCourse("course1");

        // Then:
        Assertions.assertThat(courseAssistantList.size()).isEqualTo(1);
        Assertions.assertThat(courseAssistantList.get(0).getCourseId()).isEqualTo("course1");
        Assertions.assertThat(courseAssistantList.get(0).getId()).isEqualTo("id1");
        Assertions.assertThat(courseAssistantList.get(0).getNotes()).isEqualTo("hello");
        Assertions.assertThat(courseAssistantList.get(0).getEmail()).isEqualTo("email@email.com");
        Assertions.assertThat(courseAssistantList.get(0).getBirthDate()).isEqualTo(now);
        Assertions.assertThat(courseAssistantList.get(0).getPhone()).isEqualTo("123456789");
        Assertions.assertThat(courseAssistantList.get(0).getName()).isEqualTo("name");
        Assertions.assertThat(courseAssistantList.get(0).getResponsibleName()).isEqualTo("name1");
        Assertions.assertThat(courseAssistantList.get(0).getResponsibleName2()).isEqualTo("name2");
        Assertions.assertThat(courseAssistantList.get(0).getResponsiblePhone()).isEqualTo("phone1");
        Assertions.assertThat(courseAssistantList.get(0).getResponsiblePhone2()).isEqualTo("phone2");
    }

    @Test
    public void givenAnCourseAssistant_whenSave_thenIsSaved() {
        // Given:
        CourseAssistantRepository courseAssistantRepository = Mockito.mock(CourseAssistantRepository.class);

        CourseAssistantServiceImpl courseAssistantService = new CourseAssistantServiceImpl(courseAssistantRepository);

        LocalDate now = LocalDate.now();

        CourseAssistant courseAssistant = CourseAssistant.builder()
                .id("id1")
                .courseId("course1")
                .notes("hello")
                .email("email@email.com")
                .birthDate(now)
                .phone("123456789")
                .name("name")
                .responsibleName("name1")
                .responsibleName2("name2")
                .responsiblePhone("phone1")
                .responsiblePhone2("phone2")
                .build();

        org.uresti.pozarreal.dto.CourseAssistant courseAssistantDto = org.uresti.pozarreal.dto.CourseAssistant.builder()
                .id("id1")
                .courseId("course1")
                .notes("hello")
                .email("email@email.com")
                .birthDate(now)
                .phone("123456789")
                .name("name")
                .responsibleName("name1")
                .responsibleName2("name2")
                .responsiblePhone("phone1")
                .responsiblePhone2("phone2")
                .build();

        Mockito.when(courseAssistantRepository.save(courseAssistant)).thenReturn(courseAssistant);

        // When:
        org.uresti.pozarreal.dto.CourseAssistant courseAssistantSaved = courseAssistantService.save(courseAssistantDto);

        // Then:
        Assertions.assertThat(courseAssistantSaved).isNotNull();
        Assertions.assertThat(courseAssistantSaved.getCourseId()).isEqualTo("course1");
        Assertions.assertThat(courseAssistantSaved.getId()).isEqualTo("id1");
        Assertions.assertThat(courseAssistantSaved.getNotes()).isEqualTo("hello");
        Assertions.assertThat(courseAssistantSaved.getEmail()).isEqualTo("email@email.com");
        Assertions.assertThat(courseAssistantSaved.getBirthDate()).isEqualTo(now);
        Assertions.assertThat(courseAssistantSaved.getPhone()).isEqualTo("123456789");
        Assertions.assertThat(courseAssistantSaved.getName()).isEqualTo("name");
        Assertions.assertThat(courseAssistantSaved.getResponsibleName()).isEqualTo("name1");
        Assertions.assertThat(courseAssistantSaved.getResponsibleName2()).isEqualTo("name2");
        Assertions.assertThat(courseAssistantSaved.getResponsiblePhone()).isEqualTo("phone1");
        Assertions.assertThat(courseAssistantSaved.getResponsiblePhone2()).isEqualTo("phone2");
    }

    @Test
    public void givenAnCourseAssistantWithOutCourseId_whenSave_thenIsNotSavedThrowBadRequestDataException() {
        // Given:
        CourseAssistantRepository courseAssistantRepository = Mockito.mock(CourseAssistantRepository.class);

        CourseAssistantServiceImpl courseAssistantService = new CourseAssistantServiceImpl(courseAssistantRepository);

        LocalDate now = LocalDate.now();

        CourseAssistant courseAssistant = CourseAssistant.builder()
                .id("id1")
                .notes("hello")
                .email("email@email.com")
                .birthDate(now)
                .phone("123456789")
                .name("name")
                .responsibleName("name1")
                .responsibleName2("name2")
                .responsiblePhone("phone1")
                .responsiblePhone2("phone2")
                .build();

        org.uresti.pozarreal.dto.CourseAssistant courseAssistantDto = org.uresti.pozarreal.dto.CourseAssistant.builder()
                .id("id1")
                .notes("hello")
                .email("email@email.com")
                .birthDate(now)
                .phone("123456789")
                .name("name")
                .responsibleName("name1")
                .responsibleName2("name2")
                .responsiblePhone("phone1")
                .responsiblePhone2("phone2")
                .build();

        Mockito.when(courseAssistantRepository.save(courseAssistant)).thenReturn(courseAssistant);

        // When:
        // Then:
        Assertions.assertThatThrownBy(() -> courseAssistantService.save(courseAssistantDto))
                .isInstanceOf(BadRequestDataException.class)
                .hasMessage("Invalid course id for course assistant", "INVALID_COURSE_ID");

        Mockito.verifyNoMoreInteractions(courseAssistantRepository);
    }

    @Test
    public void givenAnCourseAssistantWithOutName_whenSave_thenIsNotSavedThrowBadRequestDataException() {
        // Given:
        CourseAssistantRepository courseAssistantRepository = Mockito.mock(CourseAssistantRepository.class);
        CourseAssistantServiceImpl courseAssistantService = new CourseAssistantServiceImpl(courseAssistantRepository);

        LocalDate now = LocalDate.now();

        CourseAssistant courseAssistant = CourseAssistant.builder()
                .id("id1")
                .courseId("course1")
                .notes("hello")
                .email("email@email.com")
                .birthDate(now)
                .phone("123456789")
                .responsibleName("name1")
                .responsibleName2("name2")
                .responsiblePhone("phone1")
                .responsiblePhone2("phone2")
                .build();

        org.uresti.pozarreal.dto.CourseAssistant courseAssistantDto = org.uresti.pozarreal.dto.CourseAssistant.builder()
                .id("id1")
                .courseId("course1")
                .notes("hello")
                .email("email@email.com")
                .birthDate(now)
                .phone("123456789")
                .responsibleName("name1")
                .responsibleName2("name2")
                .responsiblePhone("phone1")
                .responsiblePhone2("phone2")
                .build();

        Mockito.when(courseAssistantRepository.save(courseAssistant)).thenReturn(courseAssistant);

        // When:
        // Then:
        Assertions.assertThatThrownBy(() -> courseAssistantService.save(courseAssistantDto))
                .isInstanceOf(BadRequestDataException.class)
                .hasMessage("Name is required for course assistant", "INVALID_COURSE_ASSISTANT_NAME");

        Mockito.verifyNoMoreInteractions(courseAssistantRepository);
    }

    @Test
    public void givenAnECourseAssistantWithEmptyId_whenGetCourseAssistant_thenIsReturnedAnListWithOneElementAndIsFilled() {
        // Given:
        CourseAssistantRepository courseAssistantRepository = Mockito.mock(CourseAssistantRepository.class);

        CourseAssistantServiceImpl courseAssistantService = new CourseAssistantServiceImpl(courseAssistantRepository);

        ArgumentCaptor<CourseAssistant> argumentCaptor =
                ArgumentCaptor.forClass(org.uresti.pozarreal.model.CourseAssistant.class);

        LocalDate now = LocalDate.now();

        CourseAssistant courseAssistant = CourseAssistant.builder()
                .id("id1")
                .courseId("course1")
                .notes("hello")
                .email("email@email.com")
                .birthDate(now)
                .phone("123456789")
                .name("name")
                .responsibleName("name1")
                .responsibleName2("name2")
                .responsiblePhone("phone1")
                .responsiblePhone2("phone2")
                .build();

        org.uresti.pozarreal.dto.CourseAssistant courseAssistantDto = org.uresti.pozarreal.dto.CourseAssistant.builder()
                .courseId("course1")
                .notes("hello")
                .email("email@email.com")
                .birthDate(now)
                .phone("123456789")
                .name("name")
                .responsibleName("name1")
                .responsibleName2("name2")
                .responsiblePhone("phone1")
                .responsiblePhone2("phone2")
                .build();

        Mockito.when(courseAssistantRepository.save(argumentCaptor.capture())).thenReturn(courseAssistant);

        // When:
        org.uresti.pozarreal.dto.CourseAssistant courseAssistantSaved = courseAssistantService.save(courseAssistantDto);

        // Then:
        org.uresti.pozarreal.model.CourseAssistant parameter = argumentCaptor.getValue();

        Assertions.assertThat(courseAssistantSaved).isNotNull();
        Assertions.assertThat(courseAssistantSaved.getId()).isNotNull();
        Assertions.assertThat(parameter.getId()).isNotNull();
        Assertions.assertThat(parameter.getId()).isNotEqualTo(courseAssistant.getId());
    }

    @Test
    public void givenAnCourseAssistantId_whenDelete_thenIsDeleted() {
        // Given:
        CourseAssistantRepository courseAssistantRepository = Mockito.mock(CourseAssistantRepository.class);

        CourseAssistantServiceImpl courseAssistantService = new CourseAssistantServiceImpl(courseAssistantRepository);

        // When:
        courseAssistantService.delete("id1");

        // Then:
        Mockito.verify(courseAssistantRepository).deleteById("id1");
    }
}