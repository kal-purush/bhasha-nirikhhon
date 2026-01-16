package com.mikaelsonbraz.parkingapi.api.address;

import com.mikaelsonbraz.parkingapi.api.address.model.entity.Address;
import com.mikaelsonbraz.parkingapi.api.address.model.repository.AddressRepository;
import com.mikaelsonbraz.parkingapi.api.city.model.entity.City;
import com.mikaelsonbraz.parkingapi.api.state.model.entity.State;
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
public class AddressRepositoryTest {

    @Autowired
    TestEntityManager entityManager;

    @Autowired
    AddressRepository repository;

    private State createNewState(){
        return State.builder().name("Rio Grande do Norte").build();
    }

    private City createNewCity(){
        return City.builder().name("Santana do Seridó").state(createNewState()).build();
    }

    private Address createNewAddress(){
        return Address.builder().cep("59350-000").city(createNewCity()).build();
    }

    @Test
    @DisplayName("Must return a Optional.of(Address) by id")
    public void findByIdTest(){
        //cenário
        Address address = createNewAddress();
        entityManager.persist(address);

        //verificação
        Optional<Address> foundAddress = repository.findById(address.getIdAddress());

        //verificação
        Assertions.assertThat(foundAddress.isPresent()).isTrue();
    }

    @Test
    @DisplayName("Must verify if Addres is correctly saved with the city and state data")
    public void saveAddressTest(){
        //cenário
        Address address = createNewAddress();

        //verificação
        Address savedAddress = repository.save(address);

        //verificação
        Assertions.assertThat(savedAddress.getIdAddress()).isNotNull();
        Assertions.assertThat(savedAddress.getCep()).isEqualTo("59350-000");
        Assertions.assertThat(savedAddress.getCity().getName()).isEqualTo("Santana do Seridó");
        Assertions.assertThat(savedAddress.getCity().getState().getName()).isEqualTo("Rio Grande do Norte");
    }
}