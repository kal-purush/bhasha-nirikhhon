package org.cyberrealm.tech.service.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.cyberrealm.tech.util.TestConstants.ACCOMMODATION;
import static org.cyberrealm.tech.util.TestConstants.ADDRESS_DUPLICATE_RESOURCE_EXCEPTION;
import static org.cyberrealm.tech.util.TestConstants.BOOKED_ADDRESS_DUPLICATE_RESOURCE_EXCEPTION;
import static org.cyberrealm.tech.util.TestConstants.ENTITY_NOT_FOUND_EXCEPTION;
import static org.cyberrealm.tech.util.TestConstants.FIRST_ACCOMMODATION_ID;
import static org.cyberrealm.tech.util.TestConstants.INVALID_ACCOMMODATION_ID;
import static org.cyberrealm.tech.util.TestUtil.ACCOMMODATION_RESPONSE_DTO;
import static org.cyberrealm.tech.util.TestUtil.CREATE_ACCOMMODATION_REQUEST_DTO;
import static org.cyberrealm.tech.util.TestUtil.CREATE_ADDRESS_REQUEST_DTO;
import static org.cyberrealm.tech.util.TestUtil.FIRST_ACCOMMODATION;
import static org.cyberrealm.tech.util.TestUtil.FIRST_ADDRESS;
import static org.cyberrealm.tech.util.TestUtil.SECOND_ADDRESS;
import static org.cyberrealm.tech.util.TestUtil.SECOND_CREATE_ACCOMMODATION_REQUEST_DTO;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;
import org.cyberrealm.tech.dto.accommodation.AccommodationDto;
import org.cyberrealm.tech.exception.DuplicateResourceException;
import org.cyberrealm.tech.exception.EntityNotFoundException;
import org.cyberrealm.tech.mapper.AccommodationMapper;
import org.cyberrealm.tech.mapper.AddressMapper;
import org.cyberrealm.tech.mapper.impl.AccommodationMapperImpl;
import org.cyberrealm.tech.mapper.impl.AddressMapperImpl;
import org.cyberrealm.tech.model.Accommodation;
import org.cyberrealm.tech.repository.AccommodationRepository;
import org.cyberrealm.tech.repository.AddressRepository;
import org.cyberrealm.tech.service.NotificationService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;

@ExtendWith(MockitoExtension.class)
class AccommodationServiceImplTest {
    @Mock
    private AccommodationRepository accommodationRepository;
    @Spy
    private AddressMapper addressMapper = new AddressMapperImpl();
    @Spy
    private AccommodationMapper accommodationMapper = new AccommodationMapperImpl(addressMapper);
    @Mock
    private AddressRepository addressRepository;
    @Mock
    private NotificationService notificationService;
    @InjectMocks
    private AccommodationServiceImpl accommodationService;

    @Test
    @DisplayName("Save a valid accommodation")
    void save_validCreateAccommodationRequestDto_returnsAccommodationDto() {
        //Given
        when(accommodationMapper.toEntity(CREATE_ACCOMMODATION_REQUEST_DTO))
                .thenReturn(FIRST_ACCOMMODATION);
        when(accommodationRepository.save(FIRST_ACCOMMODATION)).thenReturn(FIRST_ACCOMMODATION);
        when(addressRepository.existsByCountryAndCityAndStateAndStreetAndHouseNumber(
                CREATE_ADDRESS_REQUEST_DTO.country(), CREATE_ADDRESS_REQUEST_DTO.city(),
                CREATE_ADDRESS_REQUEST_DTO.state(), CREATE_ADDRESS_REQUEST_DTO.street(),
                CREATE_ADDRESS_REQUEST_DTO.houseNumber())).thenReturn(false);
        //When
        AccommodationDto actual = accommodationService.save(CREATE_ACCOMMODATION_REQUEST_DTO);
        //Then
        AccommodationDto expected = ACCOMMODATION_RESPONSE_DTO;
        assertThat(expected).usingRecursiveComparison().isEqualTo(actual);
        verify(addressRepository, times(1))
                .existsByCountryAndCityAndStateAndStreetAndHouseNumber(
                        CREATE_ADDRESS_REQUEST_DTO.country(), CREATE_ADDRESS_REQUEST_DTO.city(),
                        CREATE_ADDRESS_REQUEST_DTO.state(), CREATE_ADDRESS_REQUEST_DTO.street(),
                        CREATE_ADDRESS_REQUEST_DTO.houseNumber());
        verify(accommodationRepository, times(1)).save(FIRST_ACCOMMODATION);
        verify(accommodationMapper, times(1))
                .toEntity(CREATE_ACCOMMODATION_REQUEST_DTO);
        verify(notificationService, times(1))
                .sendNotification("New booking created with ID:" + FIRST_ACCOMMODATION.getId());
    }

    @Test
    @DisplayName("Throw DuplicateResourceException")
    void save_duplicatedAddress_throwsDuplicateResourceException() {
        //Given
        when(addressRepository.existsByCountryAndCityAndStateAndStreetAndHouseNumber(
                CREATE_ADDRESS_REQUEST_DTO.country(), CREATE_ADDRESS_REQUEST_DTO.city(),
                CREATE_ADDRESS_REQUEST_DTO.state(), CREATE_ADDRESS_REQUEST_DTO.street(),
                CREATE_ADDRESS_REQUEST_DTO.houseNumber())).thenReturn(true);
        //When
        DuplicateResourceException exception = assertThrows(DuplicateResourceException.class, () ->
                accommodationService.save(CREATE_ACCOMMODATION_REQUEST_DTO));
        String actual = exception.getMessage();
        //Then
        String expected = String.format(ADDRESS_DUPLICATE_RESOURCE_EXCEPTION,
                CREATE_ADDRESS_REQUEST_DTO.country(), CREATE_ADDRESS_REQUEST_DTO.city(),
                CREATE_ADDRESS_REQUEST_DTO.state(), CREATE_ADDRESS_REQUEST_DTO.street(),
                CREATE_ADDRESS_REQUEST_DTO.houseNumber(), CREATE_ADDRESS_REQUEST_DTO.postalCode());
        assertThat(actual).isEqualTo(expected);
        verify(addressRepository, times(1))
                .existsByCountryAndCityAndStateAndStreetAndHouseNumber(
                        CREATE_ADDRESS_REQUEST_DTO.country(), CREATE_ADDRESS_REQUEST_DTO.city(),
                        CREATE_ADDRESS_REQUEST_DTO.state(), CREATE_ADDRESS_REQUEST_DTO.street(),
                        CREATE_ADDRESS_REQUEST_DTO.houseNumber());
        verify(notificationService, times(0)).sendNotification(anyString());
    }

    @Test
    @DisplayName("Find all accommodations")
    void findAll_validPageable_returnsAllAccommodations() {
        //Given
        Pageable pageable = PageRequest.of(0, 1);
        Page<Accommodation> page = new PageImpl<>(List.of(FIRST_ACCOMMODATION), pageable, 1);
        when(accommodationRepository.findAll(pageable)).thenReturn(page);

        //When
        List<AccommodationDto> actual = accommodationService.findAll(pageable);

        //Then
        AccommodationDto expected = ACCOMMODATION_RESPONSE_DTO;
        assertThat(actual).hasSize(1);
        assertThat(actual).containsExactly(expected);
        verify(accommodationRepository, times(1)).findAll(pageable);
    }

    @Test
    @DisplayName("Find accommodation by ID")
    void findById_validId_returnsAccommodationDto() {
        //Given
        when(accommodationRepository.findById(FIRST_ACCOMMODATION_ID))
                .thenReturn(Optional.of(FIRST_ACCOMMODATION));

        //When
        AccommodationDto actual = accommodationService.findById(FIRST_ACCOMMODATION_ID);

        //Then
        AccommodationDto expected = ACCOMMODATION_RESPONSE_DTO;
        assertThat(actual).usingRecursiveComparison().isEqualTo(expected);
        verify(accommodationRepository, times(1)).findById(FIRST_ACCOMMODATION_ID);
    }

    @Test
    @DisplayName("Find accommodation by invalid id should throw EntityNotFoundException")
    void findById_invalidId_throwsEntityNotFoundException() {
        //Given
        when(accommodationRepository.findById(INVALID_ACCOMMODATION_ID))
                .thenReturn(Optional.empty());

        //When
        EntityNotFoundException exception = assertThrows(EntityNotFoundException.class, () ->
                accommodationService.findById(INVALID_ACCOMMODATION_ID));
        String actual = exception.getMessage();

        //Then
        String expected = String.format(ENTITY_NOT_FOUND_EXCEPTION, ACCOMMODATION,
                INVALID_ACCOMMODATION_ID);
        assertThat(actual).isEqualTo(expected);
        verify(accommodationRepository, times(1)).findById(INVALID_ACCOMMODATION_ID);
    }

    @Test
    @DisplayName("Update accommodation by ID")
    void updateById_validIdAndRequestDto_returnsUpdatedAccommodationDto() {
        //Given
        when(addressRepository.findByCountryAndCityAndStateAndStreetAndHouseNumber(
                FIRST_ADDRESS.getCountry(),
                FIRST_ADDRESS.getCity(),
                FIRST_ADDRESS.getState(),
                FIRST_ADDRESS.getStreet(),
                FIRST_ADDRESS.getHouseNumber()
        )).thenReturn(FIRST_ADDRESS);
        when(accommodationRepository.findById(FIRST_ACCOMMODATION_ID))
                .thenReturn(Optional.of(FIRST_ACCOMMODATION));
        when(accommodationRepository.save(FIRST_ACCOMMODATION)).thenReturn(FIRST_ACCOMMODATION);

        //When
        AccommodationDto actual = accommodationService.updateById(FIRST_ACCOMMODATION_ID,
                CREATE_ACCOMMODATION_REQUEST_DTO);

        //Then
        AccommodationDto expected = ACCOMMODATION_RESPONSE_DTO;
        assertThat(actual).usingRecursiveComparison().isEqualTo(expected);
        verify(accommodationRepository, times(1))
                .findById(FIRST_ACCOMMODATION_ID);
        verify(accommodationRepository, times(1)).save(FIRST_ACCOMMODATION);
    }

    @Test
    @DisplayName("Update accommodation by invalid id")
    void updateById_invalidId_throwsEntityNotFoundException() {
        //Given
        when(accommodationRepository.findById(INVALID_ACCOMMODATION_ID))
                .thenReturn(Optional.empty());

        //When
        EntityNotFoundException exception = assertThrows(EntityNotFoundException.class, () ->
                accommodationService.updateById(INVALID_ACCOMMODATION_ID,
                        CREATE_ACCOMMODATION_REQUEST_DTO));
        String actual = exception.getMessage();

        //Then
        String expected = String.format(ENTITY_NOT_FOUND_EXCEPTION, ACCOMMODATION,
                INVALID_ACCOMMODATION_ID);
        assertThat(actual).isEqualTo(expected);
        verify(accommodationRepository, times(1))
                .findById(INVALID_ACCOMMODATION_ID);
    }

    @Test
    @DisplayName("Update accommodation with unavailable address")
    void updateById_invalidRequestDto_throwsDuplicateResourceException() {
        //Given
        when(accommodationRepository.findById(FIRST_ACCOMMODATION_ID))
                .thenReturn(Optional.of(FIRST_ACCOMMODATION));
        when(addressRepository.findByCountryAndCityAndStateAndStreetAndHouseNumber(
                SECOND_ADDRESS.getCountry(),
                SECOND_ADDRESS.getCity(),
                SECOND_ADDRESS.getState(),
                SECOND_ADDRESS.getStreet(),
                SECOND_ADDRESS.getHouseNumber()
        )).thenReturn(SECOND_ADDRESS);

        //When
        DuplicateResourceException exception = assertThrows(DuplicateResourceException.class, () ->
                accommodationService.updateById(FIRST_ACCOMMODATION_ID,
                        SECOND_CREATE_ACCOMMODATION_REQUEST_DTO));
        String actual = exception.getMessage();

        //Then
        String expected = String.format(BOOKED_ADDRESS_DUPLICATE_RESOURCE_EXCEPTION,
                SECOND_ADDRESS.getCountry(),
                SECOND_ADDRESS.getCity(),
                SECOND_ADDRESS.getState(),
                SECOND_ADDRESS.getStreet(),
                SECOND_ADDRESS.getHouseNumber(),
                SECOND_ADDRESS.getPostalCode());
        assertThat(actual).isEqualTo(expected);
        verify(accommodationRepository, times(1))
                .findById(FIRST_ACCOMMODATION_ID);
    }

    @Test
    @DisplayName("Delete accommodation by ID")
    void deleteById_validId_deletesAccommodation() {
        //Given
        doNothing().when(accommodationRepository).deleteById(FIRST_ACCOMMODATION_ID);

        //When
        accommodationService.deleteById(FIRST_ACCOMMODATION_ID);

        //Then
        verify(accommodationRepository, times(1)).deleteById(FIRST_ACCOMMODATION_ID);
    }
}