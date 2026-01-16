package com.LuuQu.medicalclinic.mapper;

import com.LuuQu.medicalclinic.model.dto.AppointmentDto;
import com.LuuQu.medicalclinic.model.entity.Appointment;
import com.LuuQu.medicalclinic.testHelper.TestData;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;


import static org.mockito.Mockito.when;

public class AppointmentMapperTest {
    private AppointmentMapper appointmentMapper;
    private DoctorMapper doctorMapper;
    private PatientMapper patientMapper;

    @BeforeEach
    void setUp() {
        appointmentMapper = new AppointmentMapperImpl();
        doctorMapper = Mockito.mock(DoctorMapperImpl.class);
        patientMapper = Mockito.mock(PatientMapperImpl.class);
        ReflectionTestUtils.setField(appointmentMapper, "doctorMapper", doctorMapper);
        ReflectionTestUtils.setField(appointmentMapper, "patientMapper", patientMapper);
    }

    @Test
    void toDto_appointmentIsNull_NullReturned() {
        AppointmentDto result = appointmentMapper.toDto(null);

        Assertions.assertNull(result);
    }

    @Test
    void toDto_correctApproach_AppointmentDtoReturned() {
        Appointment appointment = TestData.AppointmentFactory.get(1L);
        appointment.setPatient(TestData.PatientFactory.get(1L));
        appointment.setDoctor(TestData.DoctorFactory.get(1L));
        when(patientMapper.toDto(appointment.getPatient())).thenReturn(TestData.PatientDtoFactory.get(1L));
        when(doctorMapper.toSimpleDto(appointment.getDoctor())).thenReturn(TestData.DoctorSimpleDtoFactory.get(1L));

        AppointmentDto expectedResult = TestData.AppointmentDtoFactory.get(1L);
        expectedResult.setPatient(TestData.PatientDtoFactory.get(1L));
        expectedResult.setDoctor(TestData.DoctorSimpleDtoFactory.get(1L));

        AppointmentDto result = appointmentMapper.toDto(appointment);

        Assertions.assertEquals(expectedResult, result);
    }

    @Test
    void toEntity_appointmentDtoIdNull_NullReturned() {
        Appointment result = appointmentMapper.toEntity(null);

        Assertions.assertNull(result);
    }

    @Test
    void toEntity_correctApproach_AppointmentReturned() {
        AppointmentDto appointmentDto = TestData.AppointmentDtoFactory.get(1L);
        appointmentDto.setPatient(TestData.PatientDtoFactory.get(1L));
        appointmentDto.setDoctor(TestData.DoctorSimpleDtoFactory.get(1L));
        when(patientMapper.toEntity(appointmentDto.getPatient())).thenReturn(TestData.PatientFactory.get(1L));
        when(doctorMapper.toEntity(appointmentDto.getDoctor())).thenReturn(TestData.DoctorFactory.get(1L));

        Appointment expectedResult = TestData.AppointmentFactory.get(1L);
        expectedResult.setPatient(TestData.PatientFactory.get(1L));
        expectedResult.setDoctor(TestData.DoctorFactory.get(1L));

        Appointment result = appointmentMapper.toEntity(appointmentDto);

        Assertions.assertEquals(expectedResult, result);
    }
}