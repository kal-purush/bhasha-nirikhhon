package com.LuuQu.medicalclinic.service;

import com.LuuQu.medicalclinic.mapper.PatientMapper;
import com.LuuQu.medicalclinic.model.dto.PatientDto;
import com.LuuQu.medicalclinic.model.entity.Patient;
import com.LuuQu.medicalclinic.model.entity.User;
import com.LuuQu.medicalclinic.repository.PatientRepository;
import com.LuuQu.medicalclinic.repository.UserRepository;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mapstruct.factory.Mappers;
import org.mockito.Mockito;

import java.time.LocalDate;
import java.util.Optional;

import static org.mockito.Mockito.when;

public class PatientServiceTest {
    private PatientService patientService;
    private PatientRepository patientRepository;
    private UserRepository userRepository;

    @BeforeEach
    void setUp() {
        this.patientRepository = Mockito.mock(PatientRepository.class);
        this.userRepository = Mockito.mock(UserRepository.class);
        PatientMapper patientMapper = Mappers.getMapper(PatientMapper.class);
        this.patientService = new PatientService(patientRepository, userRepository, patientMapper);
    }

    @Test
    void getPatient_noPatientInDb_CorrectException() {
        when(patientRepository.findById(1L)).thenReturn(Optional.empty());

        var exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> patientService.getPatient(1L));

        Assertions.assertEquals(exception.getMessage(), "Patient does not exist");
    }

    @Test
    void getPatient_patientFound_DtoReturned() {
        when(patientRepository.findById(1L)).thenReturn(setPatientOptional());
        PatientDto expectedResult = new PatientDto();
        expectedResult.setId(1L);

        PatientDto result = patientService.getPatient(1L);

        Assertions.assertEquals(result, expectedResult);
    }

    @Test
    void addPatient_emailTaken_CorrectException() {
        String email = "email";
        User user = new User();
        user.setEmail(email);
        when(userRepository.findByEmail(email)).thenReturn(Optional.of(user));
        PatientDto patientDto = new PatientDto();
        patientDto.setEmail(email);

        var exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> patientService.addPatient(patientDto));

        Assertions.assertEquals(exception.getMessage(), "User with given e-mail already exist");
    }

    @Test
    void addPatient_patientAdded_DtoReturned() {
        String email = "email";
        when(userRepository.findByEmail(email)).thenReturn(Optional.empty());
        PatientDto patientDto = new PatientDto();
        patientDto.setEmail(email);
        PatientDto expectedResult = new PatientDto();
        expectedResult.setEmail(email);

        PatientDto result = patientService.addPatient(patientDto);

        Assertions.assertEquals(result, expectedResult);
    }

    @Test
    void editPatient_noPatientInDb_CorrectException() {
        when(patientRepository.findById(1L)).thenReturn(Optional.empty());

        var exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> patientService.editPatient(1L, new PatientDto()));

        Assertions.assertEquals(exception.getMessage(), "Patient does not exist");
    }

    @Test
    void editPatient_patientInDb_DtoReturned() {
        when(patientRepository.findById(1L)).thenReturn(setPatientOptional());
        PatientDto patientDto = getPatientDto();
        PatientDto expectedResult = getPatientDto();
        expectedResult.setId(1L);

        PatientDto result = patientService.editPatient(1L, patientDto);

        Assertions.assertEquals(result, expectedResult);
    }

    @Test
    void editPassword_EmptyPassword_CorrectException() {
        var exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> patientService.editPassword(1L, new PatientDto()));

        Assertions.assertEquals(exception.getMessage(), "Incorrect body");
    }

    @Test
    void editPassword_noPatientInDb_CorrectException() {
        String password = "password";
        PatientDto patientPassword = new PatientDto();
        patientPassword.setPassword(password);
        when(patientRepository.findById(1L)).thenReturn(Optional.empty());

        var exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> patientService.editPassword(1L, patientPassword));

        Assertions.assertEquals(exception.getMessage(), "Patient does not exist");
    }

    @Test
    void editPassword_correctApproach_DtoReturned() {
        String password = "password";
        PatientDto patientPassword = new PatientDto();
        patientPassword.setPassword(password);
        when(patientRepository.findById(1L)).thenReturn(setPatientOptional());
        PatientDto expectedResult = new PatientDto();
        expectedResult.setPassword(password);
        expectedResult.setId(1L);

        PatientDto result = patientService.editPassword(1L, patientPassword);

        Assertions.assertEquals(result, expectedResult);
    }

    Optional<Patient> setPatientOptional() {
        Patient patient = new Patient();
        patient.setId(1L);
        return Optional.of(patient);
    }

    PatientDto getPatientDto() {
        PatientDto patientDto = new PatientDto();
        patientDto.setEmail("email");
        patientDto.setPassword("password");
        patientDto.setIdCardNo("idCardNo");
        patientDto.setFirstName("firstName");
        patientDto.setLastName("lastName");
        patientDto.setPhoneNumber("123123123");
        patientDto.setBirthday(LocalDate.of(2024, 12, 12));
        return patientDto;
    }
}