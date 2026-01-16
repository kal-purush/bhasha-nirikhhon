package uz.pdp.simline.service.impl;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uz.pdp.simline.dto.respone.PassportDetailDto;
import uz.pdp.simline.entity.PassportDetail;
import uz.pdp.simline.exception.NotFoundException;
import uz.pdp.simline.repository.PassportDetailRepository;
import java.time.LocalDate;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PassportDetailsServiceImplTest {

    @Mock
    private PassportDetailRepository passportDetailRepository;

    @InjectMocks
    private PassportDetailsServiceImpl passportDetailsService;

    @Test
    void save_ValidPassportDetailDto_ReturnsSavedDto() {
        PassportDetailDto dtoToSave = createDummyPassportDetailDto();
        PassportDetail savedEntity = createDummyPassportDetail();
        when(passportDetailRepository.save(any())).thenReturn(savedEntity);

        PassportDetailDto savedDto = passportDetailsService.save(dtoToSave);

        assertNotNull(savedDto);
        assertEquals(dtoToSave.getName(), savedDto.getName());
        assertEquals(dtoToSave.getSurname(), savedDto.getSurname());
        assertEquals(dtoToSave.getBirthDate(), savedDto.getBirthDate());
        assertEquals(dtoToSave.getPassportId(), savedDto.getPassportId());
    }

    @Test
    void update_ExistingId_ReturnsUpdatedDto() {
        PassportDetailDto dtoToUpdate = createDummyPassportDetailDto();
        PassportDetail existingEntity = createDummyPassportDetail();
        when(passportDetailRepository.findById(dtoToUpdate.getId())).thenReturn(Optional.of(existingEntity));
        when(passportDetailRepository.save(any())).thenReturn(existingEntity);

        PassportDetailDto updatedDto = passportDetailsService.update(dtoToUpdate);

        assertNotNull(updatedDto);
        assertEquals(dtoToUpdate.getName(), updatedDto.getName());
        assertEquals(dtoToUpdate.getSurname(), updatedDto.getSurname());
        assertEquals(dtoToUpdate.getBirthDate(), updatedDto.getBirthDate());
        assertEquals(dtoToUpdate.getPassportId(), updatedDto.getPassportId());
    }

    @Test
    void getById_ExistingId_ReturnsDto() {
        PassportDetail existingEntity = createDummyPassportDetail();
        when(passportDetailRepository.findById(existingEntity.getId())).thenReturn(Optional.of(existingEntity));

        PassportDetailDto foundDto = passportDetailsService.getById(existingEntity.getId());

        assertNotNull(foundDto);
        assertEquals(existingEntity.getName(), foundDto.getName());
        assertEquals(existingEntity.getSurname(), foundDto.getSurname());
        assertEquals(existingEntity.getBirthDate(), foundDto.getBirthDate());
        assertEquals(existingEntity.getPassportId(), foundDto.getPassportId());
    }

    @Test
    void getById_NonExistingId_ThrowsNotFoundException() {
        UUID nonExistingId = UUID.randomUUID();
        when(passportDetailRepository.findById(nonExistingId)).thenReturn(Optional.empty());

        assertThrows(NotFoundException.class, () -> passportDetailsService.getById(nonExistingId));
    }

    @Test
    void getAll_NotEmptyList_ReturnsListOfDtos() {
        List<PassportDetail> dummyList = Collections.singletonList(createDummyPassportDetail());
        when(passportDetailRepository.findAll()).thenReturn(dummyList);

        List<PassportDetailDto> dtoList = passportDetailsService.getAll();

        assertFalse(dtoList.isEmpty());
        assertEquals(dummyList.size(), dtoList.size());
    }

    @Test
    void delete_ExistingId_VerifyRepositoryDeleteCalled() {
        UUID existingId = UUID.randomUUID();
        PassportDetail existingEntity = createDummyPassportDetail();
        when(passportDetailRepository.findById(existingId)).thenReturn(Optional.of(existingEntity));

        passportDetailsService.delete(existingId);

        verify(passportDetailRepository, times(1)).delete(existingEntity);
    }

    private PassportDetailDto createDummyPassportDetailDto() {
        return new PassportDetailDto(UUID.randomUUID(), "John", "Doe", LocalDate.of(1990, 1, 1), "AB1234567");
    }

    private PassportDetail createDummyPassportDetail() {
        return new PassportDetail(UUID.randomUUID(), "John", "Doe", LocalDate.of(1990, 1, 1), "AB1234567");
    }
}