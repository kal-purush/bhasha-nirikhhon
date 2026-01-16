package com.mikaelsonbraz.parkingapi.api.parkingSpace.repository;

import com.mikaelsonbraz.parkingapi.api.parkingSpace.model.entity.ParkingSpace;
import com.mikaelsonbraz.parkingapi.api.parkingSpace.model.repository.ParkingSpaceRepository;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.Optional;

@ExtendWith(SpringExtension.class)
@ActiveProfiles("test")
@DataJpaTest
public class ParkingSpaceRepositoryTest {

    @Autowired
    TestEntityManager entityManager;

    @Autowired
    ParkingSpaceRepository repository;

    public ParkingSpace createParkingSpace(){
        return ParkingSpace.builder().spaceType(1).build();
    }

    @Test
    @DisplayName("Find Parking space by Id test")
    public void findByIdTest(){
        //cenário
        ParkingSpace space = createParkingSpace();
        entityManager.persist(space);

        //execução
        Optional<ParkingSpace> foundSpace = repository.findById(space.getIdSpace());

        //verificação
        Assertions.assertThat(foundSpace.isPresent()).isTrue();
    }
}