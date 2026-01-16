package com.LuuQu.medicalclinic.service;

import com.LuuQu.medicalclinic.mapper.DoctorMapper;
import com.LuuQu.medicalclinic.model.dto.DoctorDto;
import com.LuuQu.medicalclinic.model.dto.FacilitySimpleDto;
import com.LuuQu.medicalclinic.model.entity.Doctor;
import com.LuuQu.medicalclinic.model.entity.Facility;
import com.LuuQu.medicalclinic.repository.DoctorRepository;
import com.LuuQu.medicalclinic.repository.FacilityRepository;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mapstruct.factory.Mappers;
import org.mockito.Mockito;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.mockito.Mockito.when;

public class DoctorServiceTest {
    private DoctorService doctorService;
    private DoctorRepository doctorRepository;
    private FacilityRepository facilityRepository;
    private DoctorMapper doctorMapper;

    @BeforeEach
    void setUp() {
        this.doctorRepository = Mockito.mock(DoctorRepository.class);
        this.facilityRepository = Mockito.mock(FacilityRepository.class);
        doctorMapper = Mappers.getMapper(DoctorMapper.class);
        doctorService = new DoctorService(doctorRepository, facilityRepository, doctorMapper);
    }

    @Test
    void getDoctor_noDoctorInDb_CorrectException() {
        when(doctorRepository.findById(1L)).thenReturn(Optional.empty());

        var exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> doctorService.getDoctor(1L));

        Assertions.assertEquals(exception.getMessage(), "Non-existent doctor");
    }

    @Test
    void getDoctor_doctorFound_DtoReturned() {
        when(doctorRepository.findById(1L)).thenReturn(setDoctorOptional());
        DoctorDto expectedResult = new DoctorDto();
        expectedResult.setId(1L);

        DoctorDto result = doctorService.getDoctor(1L);

        Assertions.assertEquals(result, expectedResult);
    }

    @Test
    void addDoctor_doctorAdded_DtoReturned() {
        DoctorDto data = getDoctorDto();
        data.setId(1L);
        DoctorDto expectedResult = getDoctorDto();
        expectedResult.setId(1L);

        DoctorDto result = doctorService.addDoctor(data);

        Assertions.assertEquals(expectedResult, result);
    }

    @Test
    void editDoctor_noDoctorInDb_CorrectException() {
        when(doctorRepository.findById(1L)).thenReturn(Optional.empty());

        var exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> doctorService.editDoctor(1L, new DoctorDto()));

        Assertions.assertEquals(exception.getMessage(), "Non-existent doctor");
    }

    @Test
    void editDoctor_correctApproach_DtoReturned() {
        Doctor doctor = new Doctor();
        doctor.setId(1L);
        when(doctorRepository.findById(1L)).thenReturn(Optional.of(doctor));
        DoctorDto expectedResult = getDoctorDto();
        expectedResult.setId(1L);

        DoctorDto result = doctorService.editDoctor(1L, getDoctorDto());

        Assertions.assertEquals(expectedResult, result);
    }

    @Test
    void addDoctorFacility_noDoctorInDb_CorrectException() {
        when(doctorRepository.findById(1L)).thenReturn(Optional.empty());

        var exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> doctorService.addDoctorFacility(1L, 1L));

        Assertions.assertEquals(exception.getMessage(), "Non-existent doctor");
    }

    @Test
    void addDoctorFacility_noFacilityInDb_CorrectException() {
        when(doctorRepository.findById(1L)).thenReturn(Optional.of(new Doctor()));
        when(facilityRepository.findById(1L)).thenReturn(Optional.empty());

        var exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> doctorService.addDoctorFacility(1L, 1L));

        Assertions.assertEquals(exception.getMessage(), "Non-existent facility");
    }
    @Test
    void addDoctorFacility_correctApproach_DtoReturned() {
        Doctor doctor = doctorMapper.toEntity(getDoctorDto());
        doctor.setId(1L);
        addTwoFacilitiesToSet(doctor);
        Facility facility = getFacility();
        facility.setId(3L);
        when(doctorRepository.findById(1L)).thenReturn(Optional.of(doctor));
        when(facilityRepository.findById(3L)).thenReturn(Optional.of(facility));
        Set<Long> expectedIds = new HashSet<>();
        expectedIds.add(1L);
        expectedIds.add(2L);
        expectedIds.add(3L);

        DoctorDto result = doctorService.addDoctorFacility(1L, 3L);

        Assertions.assertEquals(doctor.getId(), result.getId());
        Assertions.assertEquals(doctor.getFacilities().size(), result.getFacilities().size());
        Set<Long> actualIds = result.getFacilities().stream()
                                .map(FacilitySimpleDto::getId)
                                .collect(Collectors.toSet());
        Assertions.assertTrue(actualIds.containsAll(expectedIds));
    }

    @Test
    void removeDoctorFacility_noDoctorInDb_CorrectException() {
        when(doctorRepository.findById(1L)).thenReturn(Optional.empty());

        var exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> doctorService.removeDoctorFacility(1L, 1L));

        Assertions.assertEquals(exception.getMessage(), "Non-existent doctor");
    }

    @Test
    void removeDoctorFacility_noFacilityInDb_CorrectException() {
        when(doctorRepository.findById(1L)).thenReturn(Optional.of(new Doctor()));
        when(facilityRepository.findById(1L)).thenReturn(Optional.empty());

        var exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> doctorService.removeDoctorFacility(1L, 1L));

        Assertions.assertEquals(exception.getMessage(), "Non-existent facility");
    }
    @Test
    void removeDoctorFacility_correctApproach_DtoReturned() {
        Doctor doctor = doctorMapper.toEntity(getDoctorDto());
        doctor.setId(1L);
        addTwoFacilitiesToSet(doctor);
        Facility facility = getFacility();
        facility.setId(2L);
        when(doctorRepository.findById(1L)).thenReturn(Optional.of(doctor));
        when(facilityRepository.findById(2L)).thenReturn(Optional.of(facility));
        Set<Long> expectedIds = new HashSet<>();
        expectedIds.add(1L);

        DoctorDto result = doctorService.removeDoctorFacility(1L, 2L);

        Assertions.assertEquals(doctor.getId(), result.getId());
        Assertions.assertEquals(doctor.getFacilities().size(), result.getFacilities().size());
        Set<Long> actualIds = result.getFacilities().stream()
                .map(FacilitySimpleDto::getId)
                .collect(Collectors.toSet());
        Assertions.assertTrue(actualIds.containsAll(expectedIds));
    }
    Optional<Doctor> setDoctorOptional() {
        Doctor doctor = new Doctor();
        doctor.setId(1L);
        return Optional.of(doctor);
    }

    DoctorDto getDoctorDto() {
        DoctorDto doctorDto = new DoctorDto();
        doctorDto.setEmail("email");
        doctorDto.setPassword("password");
        doctorDto.setFirstName("firstName");
        doctorDto.setLastName("lastName");
        doctorDto.setSpecialization("specialization");
        return doctorDto;
    }
    Facility getFacility() {
        Facility facility = new Facility();
        facility.setName("facility");
        facility.setCity("city");
        facility.setZipCode("zipCode");
        facility.setStreet("street");
        facility.setBuildingNo("123");
        return facility;
    }
    void addTwoFacilitiesToSet(Doctor doctor) {
        Facility facility1 = getFacility();
        facility1.setId(1L);
        Facility facility2 = getFacility();
        facility2.setId(2L);
        doctor.getFacilities().add(facility1);
        doctor.getFacilities().add(facility2);
    }
}