package com.LuuQu.medicalclinic.service;

import com.LuuQu.medicalclinic.mapper.DoctorMapper;
import com.LuuQu.medicalclinic.mapper.FacilityMapper;
import com.LuuQu.medicalclinic.model.dto.FacilityDto;
import com.LuuQu.medicalclinic.model.entity.Facility;
import com.LuuQu.medicalclinic.repository.FacilityRepository;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mapstruct.factory.Mappers;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Optional;

import static org.mockito.Mockito.when;

public class FacilityServiceTest {
    private FacilityService facilityService;
    private FacilityRepository facilityRepository;

    @BeforeEach
    void setUp() {
        this.facilityRepository = Mockito.mock(FacilityRepository.class);
        FacilityMapper facilityMapper = Mappers.getMapper(FacilityMapper.class);
        DoctorMapper doctorMapper = Mappers.getMapper(DoctorMapper.class);
        ReflectionTestUtils.setField(facilityMapper, "doctorMapper", doctorMapper);
        this.facilityService = new FacilityService(facilityRepository, facilityMapper);
    }

    @Test
    void getFacility_noFacilityInDb_CorrectException() {
        when(facilityRepository.findById(1L)).thenReturn(Optional.empty());

        var exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> facilityService.getFacility(1L));

        Assertions.assertEquals(exception.getMessage(), "Non-existent facility");
    }

    @Test
    void getFacility_correctApproach_DtoReturned() {
        when(facilityRepository.findById(1L)).thenReturn(getFacilityOptional());
        FacilityDto expectedResult = getFacilityDto(1L);

        FacilityDto result = facilityService.getFacility(1L);

        Assertions.assertEquals(expectedResult, result);
    }

    @Test
    void addFacility_correctApproach_DtoReturned() {
        FacilityDto result = facilityService.addFacility(getFacilityDto(1L));

        Assertions.assertEquals(getFacilityDto(1L), result);
    }

    @Test
    void editFacility_noFacilityInDb_CorrectException() {
        when(facilityRepository.findById(1L)).thenReturn(Optional.empty());

        var exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> facilityService.editFacility(1L, new FacilityDto()));

        Assertions.assertEquals(exception.getMessage(), "Non-existent facility");
    }

    @Test
    void editFacility_correctApproach_DtoReturned() {
        FacilityDto facilityDto = getFacilityDto(null);
        Facility facility = new Facility();
        facility.setId(1L);
        when(facilityRepository.findById(1L)).thenReturn(Optional.of(facility));

        FacilityDto result = facilityService.editFacility(1L, facilityDto);

        Assertions.assertEquals(getFacilityDto(1L), result);
    }

    Optional<Facility> getFacilityOptional() {
        Facility facility = new Facility();
        facility.setId(1L);
        facility.setName("Name");
        facility.setCity("City");
        facility.setZipCode("zipCode");
        facility.setStreet("street");
        facility.setBuildingNo("123");
        return Optional.of(facility);
    }

    FacilityDto getFacilityDto(Long id) {
        FacilityDto facilityDto = new FacilityDto();
        facilityDto.setId(id);
        facilityDto.setName("Name");
        facilityDto.setCity("City");
        facilityDto.setZipCode("zipCode");
        facilityDto.setStreet("street");
        facilityDto.setBuildingNo("123");
        return facilityDto;
    }
}