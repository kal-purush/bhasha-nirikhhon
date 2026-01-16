package org.uresti.pozarreal.service.impl;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.uresti.pozarreal.dto.CoursePayment;
import org.uresti.pozarreal.repository.CourseAssistantPaymentRepository;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.LinkedList;
import java.util.List;

public class CourseAssistantPaymentServiceImplTests {
    @Test
    public void givenAnEmptyCoursePayment_whenFindAllByCourseAssistant_thenEmptyListIsReturned() {
        // Given:
        CourseAssistantPaymentRepository courseAssistantPaymentRepository = Mockito.mock(CourseAssistantPaymentRepository.class);

        CourseAssistantPaymentServiceImpl courseAssistantPaymentService = new CourseAssistantPaymentServiceImpl(courseAssistantPaymentRepository);

        List<org.uresti.pozarreal.model.CoursePayment> coursePaymentList = new LinkedList<>();

        Mockito.when(courseAssistantPaymentRepository.findAllByCourseAssistantIdOrderByPaymentDateDesc(null)).thenReturn(coursePaymentList);

        // When:
        List<CoursePayment> courses = courseAssistantPaymentService.findAllByCourseAssistant(null);

        // Then:
        Assertions.assertThat(courses.isEmpty()).isTrue();
    }

    @Test
    public void givenAnCoursePaymentWithTwoElements_whenFindAllByCourseAssistant_thenListWithTwoElementsIsReturned() {
        // Given:
        CourseAssistantPaymentRepository courseAssistantPaymentRepository = Mockito
                .mock(CourseAssistantPaymentRepository.class);

        CourseAssistantPaymentServiceImpl courseAssistantPaymentService = new
                CourseAssistantPaymentServiceImpl(courseAssistantPaymentRepository);

        List<org.uresti.pozarreal.model.CoursePayment> coursePaymentList = new LinkedList<>();
        LocalDate now = LocalDate.now();

        org.uresti.pozarreal.model.CoursePayment coursePayment1 = org.uresti.pozarreal.model.CoursePayment.builder()
                .id("id1")
                .paymentDate(now)
                .amount(new BigDecimal(50))
                .courseAssistantId("abc")
                .receiptNumber("12345")
                .notes("hello")
                .userId("User 1")
                .concept("Concept 1")
                .build();

        org.uresti.pozarreal.model.CoursePayment coursePayment2 = org.uresti.pozarreal.model.CoursePayment.builder()
                .id("id2")
                .paymentDate(now)
                .amount(new BigDecimal(50))
                .courseAssistantId("abc")
                .receiptNumber("12345")
                .notes("hello")
                .userId("User 2")
                .concept("Concept 2")
                .build();

        coursePaymentList.add(coursePayment1);
        coursePaymentList.add(coursePayment2);

        Mockito.when(courseAssistantPaymentRepository.findAllByCourseAssistantIdOrderByPaymentDateDesc("abc"))
                .thenReturn(coursePaymentList);

        // When:
        List<CoursePayment> courseAssistant = courseAssistantPaymentService
                .findAllByCourseAssistant("abc");

        // Then:
        Assertions.assertThat(courseAssistant.size()).isEqualTo(2);
        Assertions.assertThat(courseAssistant.get(0).getId()).isEqualTo("id1");
        Assertions.assertThat(courseAssistant.get(0).getPaymentDate()).isEqualTo(now);
        Assertions.assertThat(courseAssistant.get(0).getUserId()).isEqualTo("User 1");
        Assertions.assertThat(courseAssistant.get(0).getAmount()).isEqualTo(new BigDecimal(50));
        Assertions.assertThat(courseAssistant.get(0).getCourseAssistantId()).isEqualTo("abc");
        Assertions.assertThat(courseAssistant.get(0).getReceiptNumber()).isEqualTo("12345");
        Assertions.assertThat(courseAssistant.get(0).getNotes()).isEqualTo("hello");
        Assertions.assertThat(courseAssistant.get(0).getConcept()).isEqualTo("Concept 1");
        Assertions.assertThat(courseAssistant.get(1).getId()).isEqualTo("id2");
        Assertions.assertThat(courseAssistant.get(1).getPaymentDate()).isEqualTo(now);
        Assertions.assertThat(courseAssistant.get(1).getUserId()).isEqualTo("User 2");
        Assertions.assertThat(courseAssistant.get(1).getAmount()).isEqualTo(new BigDecimal(50));
        Assertions.assertThat(courseAssistant.get(1).getCourseAssistantId()).isEqualTo("abc");
        Assertions.assertThat(courseAssistant.get(1).getReceiptNumber()).isEqualTo("12345");
        Assertions.assertThat(courseAssistant.get(1).getNotes()).isEqualTo("hello");
        Assertions.assertThat(courseAssistant.get(1).getConcept()).isEqualTo("Concept 2");
    }

    @Test
    public void givenAnCoursePayment_whenSave_thenIsSaved() {
        // Given:
        CourseAssistantPaymentRepository courseAssistantPaymentRepository = Mockito
                .mock(CourseAssistantPaymentRepository.class);

        CourseAssistantPaymentServiceImpl courseAssistantPaymentService = new
                CourseAssistantPaymentServiceImpl(courseAssistantPaymentRepository);

        LocalDate now = LocalDate.now();

        org.uresti.pozarreal.model.CoursePayment coursePayment = org.uresti.pozarreal.model.CoursePayment.builder()
                .id("id1")
                .paymentDate(now)
                .amount(new BigDecimal(50))
                .courseAssistantId("abc")
                .receiptNumber("12345")
                .notes("hello")
                .userId("User 1")
                .concept("Concept 1")
                .build();

        CoursePayment coursePaymentDto = CoursePayment.builder()
                .id("id1")
                .paymentDate(now)
                .amount(new BigDecimal(50))
                .courseAssistantId("abc")
                .receiptNumber("12345")
                .notes("hello")
                .userId("User 1")
                .concept("Concept 1")
                .build();

        Mockito.when(courseAssistantPaymentRepository.save(coursePayment)).thenReturn(coursePayment);

        // When:
        CoursePayment courseAssistant = courseAssistantPaymentService
                .save(coursePaymentDto);

        // Then:
        Assertions.assertThat(courseAssistant).isNotNull();
        Assertions.assertThat(courseAssistant.getId()).isEqualTo("id1");
        Assertions.assertThat(courseAssistant.getPaymentDate()).isEqualTo(now);
        Assertions.assertThat(courseAssistant.getUserId()).isEqualTo("User 1");
        Assertions.assertThat(courseAssistant.getAmount()).isEqualTo(new BigDecimal(50));
        Assertions.assertThat(courseAssistant.getCourseAssistantId()).isEqualTo("abc");
        Assertions.assertThat(courseAssistant.getReceiptNumber()).isEqualTo("12345");
        Assertions.assertThat(courseAssistant.getNotes()).isEqualTo("hello");
        Assertions.assertThat(courseAssistant.getConcept()).isEqualTo("Concept 1");
    }

    @Test
    public void givenAnCoursePaymentIdNull_whenSave_thenIsSavedAndSetId() {
        // Given:
        CourseAssistantPaymentRepository courseAssistantPaymentRepository = Mockito
                .mock(CourseAssistantPaymentRepository.class);

        CourseAssistantPaymentServiceImpl courseAssistantPaymentService = new
                CourseAssistantPaymentServiceImpl(courseAssistantPaymentRepository);

        ArgumentCaptor<org.uresti.pozarreal.model.CoursePayment> argumentCaptor =
                ArgumentCaptor.forClass(org.uresti.pozarreal.model.CoursePayment.class);

        LocalDate now = LocalDate.now();

        org.uresti.pozarreal.model.CoursePayment coursePayment = org.uresti.pozarreal.model.CoursePayment.builder()
                .id("id1")
                .paymentDate(now)
                .amount(new BigDecimal(50))
                .courseAssistantId("abc")
                .receiptNumber("12345")
                .notes("hello")
                .userId("User 1")
                .concept("Concept 1")
                .build();

        CoursePayment coursePaymentDto = CoursePayment.builder()
                .paymentDate(now)
                .amount(new BigDecimal(60))
                .courseAssistantId("abc")
                .receiptNumber("123456")
                .notes("hello2")
                .userId("User 2")
                .concept("Concept 2")
                .build();

        Mockito.when(courseAssistantPaymentRepository.save(argumentCaptor.capture())).thenReturn(coursePayment);

        // When:
        CoursePayment courseAssistant = courseAssistantPaymentService.save(coursePaymentDto);

        // Then:
        org.uresti.pozarreal.model.CoursePayment parameter = argumentCaptor.getValue();

        Assertions.assertThat(parameter.getId()).isNotNull();
        Assertions.assertThat(parameter.getPaymentDate()).isEqualTo(now);
        Assertions.assertThat(parameter.getUserId()).isEqualTo("User 2");
        Assertions.assertThat(parameter.getCourseAssistantId()).isEqualTo("abc");
        Assertions.assertThat(parameter.getReceiptNumber()).isEqualTo("123456");
        Assertions.assertThat(parameter.getAmount()).isEqualTo(new BigDecimal(60));
        Assertions.assertThat(parameter.getNotes()).isEqualTo("hello2");
        Assertions.assertThat(parameter.getConcept()).isEqualTo("Concept 2");

        Assertions.assertThat(courseAssistant.getPaymentDate()).isEqualTo(now);
        Assertions.assertThat(courseAssistant.getId()).isEqualTo("id1");
        Assertions.assertThat(courseAssistant.getUserId()).isEqualTo("User 1");
        Assertions.assertThat(courseAssistant.getCourseAssistantId()).isEqualTo("abc");
        Assertions.assertThat(courseAssistant.getReceiptNumber()).isEqualTo("12345");
        Assertions.assertThat(courseAssistant.getAmount()).isEqualTo(new BigDecimal(50));
        Assertions.assertThat(courseAssistant.getNotes()).isEqualTo("hello");
        Assertions.assertThat(courseAssistant.getConcept()).isEqualTo("Concept 1");

        Assertions.assertThat(courseAssistant.getId()).isNotEqualTo(parameter.getId());
    }

    @Test
    public void givenAnCourseAssistantPaymentId_whenDelete_thenIsDeleted() {
        // Given:
        CourseAssistantPaymentRepository courseAssistantPaymentRepository = Mockito
                .mock(CourseAssistantPaymentRepository.class);

        CourseAssistantPaymentServiceImpl courseAssistantPaymentService = new
                CourseAssistantPaymentServiceImpl(courseAssistantPaymentRepository);

        // When:
        courseAssistantPaymentService.delete("abc");

        // Then:
        Mockito.verify(courseAssistantPaymentRepository).deleteById("abc");
    }
}