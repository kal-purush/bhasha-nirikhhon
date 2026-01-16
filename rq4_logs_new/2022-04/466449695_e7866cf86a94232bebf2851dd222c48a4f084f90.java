package com.mikaelsonbraz.parkingapi.api.parkingSpace.service;

import com.mikaelsonbraz.parkingapi.api.parkingSpace.model.entity.ParkingSpace;
import com.mikaelsonbraz.parkingapi.api.parkingSpace.model.repository.ParkingSpaceRepository;
import com.mikaelsonbraz.parkingapi.api.parkingSpace.model.spaceTypeENUM.SpaceType;
import com.mikaelsonbraz.parkingapi.api.parkingSpace.service.impl.ParkingSpaceServiceImpl;
import com.mikaelsonbraz.parkingapi.api.renting.model.entity.Renting;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.time.LocalDateTime;
import java.util.Optional;

@ExtendWith(SpringExtension.class)
@ActiveProfiles("test")
public class ParkingSpaceServiceTest {

    ParkingSpaceService service;

    @MockBean
    ParkingSpaceRepository repository;

    public ParkingSpace createNewSpace(){
        return ParkingSpace.builder().idSpace(1).occupied(false).spaceType(0).build();
    }

    public Renting createNewRenting(){
        return Renting.builder().idRenting(1).entryDate(LocalDateTime.now()).hourPrice(2).dayPrice(20).build();
    }

    @BeforeEach
    public void setUp(){
        this.service = new ParkingSpaceServiceImpl(repository);
    }

    @Test
    @DisplayName("Must save a ParkingSpace")
    public void saveParkingSpaceTest(){
        //cenário
        ParkingSpace space = createNewSpace();
        Mockito.when(repository.save(space)).thenReturn(space);

        //execução
        ParkingSpace savedSpace = service.save(space);

        //verificação
        Assertions.assertThat(savedSpace.getIdSpace()).isNotNull();
        Assertions.assertThat(savedSpace.isOccupied()).isFalse();
        Assertions.assertThat(savedSpace.getSpaceType()).isEqualTo(space.getSpaceType());
        Assertions.assertThat(savedSpace.getSpaceTypeDescription()).isEqualTo("Covered space");
    }

    @Test
    @DisplayName("Must throw an IllegalArgumentException when spaceType not is between 1 and 2")
    public void shouldThrowErrorWhenIllegalSpaceType(){
        //cenário
        ParkingSpace space = createNewSpace();

        //execução
        Throwable exception = Assertions.catchThrowable(() -> space.setSpaceType(SpaceType.toEnum(3)));

        //verificação
        Assertions.assertThat(exception)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid code: " + 3);
    }

    @Test
    @DisplayName("Must obtain a ParkingSpace by id")
    public void getParkingSpaceByIdTest(){
        //cenário
        ParkingSpace space = createNewSpace();
        Mockito.when(repository.findById(1)).thenReturn(Optional.of(space));

        //execução
        Optional<ParkingSpace> foundSpace = service.getById(1);

        //verificação
        Assertions.assertThat(foundSpace.isPresent()).isTrue();
        Assertions.assertThat(foundSpace.get().isOccupied()).isFalse();
        Assertions.assertThat(foundSpace.get().getSpaceTypeDescription()).isEqualTo("Covered space");
    }

    @Test
    @DisplayName("Must return null when is no ParkingSpace in getById()")
    public void parkingSpaceNotFoundByIdTest(){
        //cenário
        Mockito.when(repository.findById(Mockito.anyInt())).thenReturn(Optional.empty());

        //execução
        Optional<ParkingSpace> foundSpace = service.getById(1);

        //verificação
        Assertions.assertThat(foundSpace.isPresent()).isFalse();
    }

    @Test
    @DisplayName("Must save a Parking Space with a renting without departure date")
    public void shouldSaveARentingOnAParkingSpace(){
        //cenário
        ParkingSpace space = createNewSpace();
        Renting renting = createNewRenting();
        space.setRenting(renting);
        Mockito.when(repository.save(space)).thenReturn(space);

        //execução
        ParkingSpace savedSpace = service.save(space);

        //execução
        Assertions.assertThat(savedSpace.isOccupied()).isTrue();
        Assertions.assertThat(savedSpace.getRenting().getIdRenting()).isEqualTo(renting.getIdRenting());
    }

    @Test
    @DisplayName("Must update a Renting on Parking Space with departure date and turn occupied atribute to false")
    public void shouldUpdateParkingSpaceWithRentingDepartureDate(){
        //cenário
        ParkingSpace space = createNewSpace();
        Renting renting = createNewRenting();
        renting.setDepartureDate(LocalDateTime.now());
        space.setRenting(renting);
        Mockito.when(repository.save(space)).thenReturn(space);

        //execução
        ParkingSpace savedSpace = service.update(space);

        //verificação
        Assertions.assertThat(savedSpace.getIdSpace()).isNotNull();
        Assertions.assertThat(savedSpace.isOccupied()).isFalse();
        Assertions.assertThat(savedSpace.getRenting()).isNull();
    }
}