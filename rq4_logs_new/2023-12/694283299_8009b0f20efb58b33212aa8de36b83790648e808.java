package com.myproject.bikereviewapp.service.impl;

import com.myproject.bikereviewapp.entity.Brand;
import com.myproject.bikereviewapp.exceptionhandler.exception.EntityNotFoundException;
import com.myproject.bikereviewapp.repository.BrandRepository;
import com.myproject.bikereviewapp.service.abstraction.BrandService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SpringBootTest(classes = BrandServiceImpl.class)
class BrandServiceImplTest {
    
    @MockBean
    BrandRepository brandRepository;

    @Autowired
    BrandService brandService;

    @Test
    void getById_shouldReturnBrand_whenBrandWasFound() {
        Long id = 10L;
        Brand brand = new Brand(id, "someName", "someCountry");

        when(brandRepository.findById(id)).thenReturn(Optional.of(brand));

        assertEquals(brand, brandService.getById(id));

        verify(brandRepository).findById(id);
    }

    @Test
    void getById_shouldThrowException_whenBrandWasNotFound() {
        Long id = 10L;

        when(brandRepository.findById(id)).thenReturn(Optional.empty());

        assertThrows(EntityNotFoundException.class, () -> brandService.getById(id));

        verify(brandRepository).findById(id);
    }

    @Test
    void update_shouldSaveAndReturnUpdatedBrand() {
        Long id = 10L;

        Brand brand = new Brand(id, "someName", "someCountry");
        Brand updatedBrand = new Brand(id, "updatedName", "updatedCountry");

        when(brandRepository.findById(id)).thenReturn(Optional.of(brand));
        when(brandRepository.save(updatedBrand)).thenReturn(updatedBrand);

        assertEquals(updatedBrand, brandService.update(id, updatedBrand));

        verify(brandRepository).save(updatedBrand);
    }

    @Test
    void update_shouldSaveAndReturnUpdatedBrand_whenBrandWasReceivedWithoutId() {
        Long id = 10L;

        Brand brand = new Brand(id, "someName", "someCountry");
        Brand receivedBrand = new Brand(null, "updatedName", "updatedCountry");
        Brand updatedBrand = new Brand(id, receivedBrand.getName(), receivedBrand.getCountry());

        when(brandRepository.findById(id)).thenReturn(Optional.of(brand));
        when(brandRepository.save(updatedBrand)).thenReturn(updatedBrand);

        assertEquals(updatedBrand, brandService.update(id, receivedBrand));

        verify(brandRepository).save(updatedBrand);
    }

}