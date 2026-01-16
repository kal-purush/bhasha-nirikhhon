package com.albydaa.equinefarm.repostiroy;

import com.albydaa.equinefarm.model.Doctor;
import com.albydaa.equinefarm.repository.DoctorRepo;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.jdbc.EmbeddedDatabaseConnection;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;

import java.util.*;

@DataJpaTest
@AutoConfigureTestDatabase(connection = EmbeddedDatabaseConnection.H2)
public class DoctorRepoTest {
    @Autowired
    private DoctorRepo doctorRepository;

    @Test
    public void DoctorRepository_FindBySpecialization_ReturnDoctorList(){
        // Arrange
        Doctor doctor = Doctor.builder()
                .id(1L)
                .salary(2141D)
                .firstName("hamza")
                .lastName("test")
                .specialization(Doctor.Specialization.SURGEON)
                .managedHorses(null)
                .build();
        Doctor doctor2 = Doctor.builder()
                .id(2L)
                .salary(2141D)
                .firstName("hamza")
                .lastName("test")
                .specialization(Doctor.Specialization.TECHNICIAN)
                .managedHorses(null)
                .build();
        doctorRepository.save(doctor);
        doctorRepository.save(doctor2);
        // Act
        List<Doctor> result = doctorRepository
                .findDoctorBySpecialization(Doctor.Specialization.SURGEON);
        // Assert
        Assertions.assertThat(result.size()).isEqualTo(1);
        Assertions.assertThat(result.get(0).getId()).isEqualTo(1);
    }
}