package com.example.mapper;

import com.example.dto.PaymentDTO;
import com.example.dto.StudentDTO;
import com.example.enumeration.PaymentStatus;
import com.example.enumeration.PaymentType;
import com.example.model.Payment;
import com.example.model.Student;
import org.junit.jupiter.api.Test;
import java.time.LocalDate;
import java.util.UUID;
import static org.assertj.core.api.Assertions.assertThat;

public class MapperImplTest {
    private final MapperImpl mapper = new MapperImpl();

    @Test
    public void testFromStudent() {
        String id = UUID.randomUUID().toString();
        Student student = Student.builder()
            .id(id)
            .code("STD01")
            .firstName("Vincent")
            .lastName("Faye")
            .programId("Azerty")
            .build();

        StudentDTO studentDTO = mapper.fromStudent(student);
        assertThat(studentDTO).isNotNull();
        assertThat(studentDTO.getId()).isEqualTo(id);
        assertThat(studentDTO.getFirstName()).isEqualTo("Vincent");
    }

    @Test
    public void testFromStudentDTO() {
        String id = UUID.randomUUID().toString();
        StudentDTO studentDTO = StudentDTO.builder()
                .id(id)
                .code("STD02")
                .firstName("Clement")
                .lastName("Faye")
                .programId("INFO01")
                .build();

        Student student = mapper.fromStudentDTO(studentDTO);
        assertThat(student).isNotNull();
        assertThat(student.getId()).isEqualTo(id);
        assertThat(student.getFirstName()).isEqualTo("Clement");
    }

    @Test
    public void testFromPayment() {
        String id = UUID.randomUUID().toString();
        Student student = Student.builder()
                .id(id)
                .code("STD01")
                .firstName("Vincent")
                .lastName("Faye")
                .programId("Azerty")
                .build();

        Payment payment = Payment.builder()
            .id(1L)
            .amount(12.0)
            .type(PaymentType.CASH)
            .status(PaymentStatus.CREATED)
            .student(student)
            .date(LocalDate.now())
        .build();

        PaymentDTO paymentDTO = mapper.fromPayment(payment);
        assertThat(paymentDTO).isNotNull();
        assertThat(paymentDTO.getId()).isEqualTo(1L);
        assertThat(paymentDTO.getAmount()).isEqualTo(12.0);
    }

    @Test
    public void testFromPaymentDTO() {
        String id = UUID.randomUUID().toString();
        StudentDTO studentDTO = StudentDTO.builder()
                .id(id)
                .code("STD02")
                .firstName("Clement")
                .lastName("Faye")
                .programId("INFO01")
                .build();

        PaymentDTO paymentDTO = PaymentDTO.builder()
                .id(1L)
                .amount(12.0)
                .type(PaymentType.CASH)
                .status(PaymentStatus.CREATED)
                .studentDTO(studentDTO)
                .date(LocalDate.now())
                .build();

        Payment payment = mapper.fromPaymentDTO(paymentDTO);
        assertThat(payment).isNotNull();
        assertThat(payment.getId()).isEqualTo(1L);
        assertThat(payment.getAmount()).isEqualTo(12.0);
    }
}