package dev.forkingaround.zootopia.controllers;

import dev.forkingaround.zootopia.models.Family;
import dev.forkingaround.zootopia.repositories.FamilyRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.Optional;
import java.util.List;
import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class FamilyControllerTest {

    @Mock
    private FamilyRepository familyRepository;

    @InjectMocks
    private FamilyController familyController;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testGetAllFamilies() {

        Family family1 = new Family(1L, "Carnivores", new HashSet<>());
        Family family2 = new Family(2L, "Herbivores", new HashSet<>());
        List<Family> families = List.of(family1, family2);

        when(familyRepository.findAll()).thenReturn(families);

        ResponseEntity<List<Family>> response = familyController.getAllFamilies();

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(families, response.getBody());
        verify(familyRepository, times(1)).findAll();
    }

    @Test
    void testGetFamilyById_Found() throws Exception {

        Family family = new Family(1L, "Carnivores", new HashSet<>());
        when(familyRepository.findById(1L)).thenReturn(Optional.of(family));

        ResponseEntity<Family> response = familyController.getFamilyById(1L);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(family, response.getBody());
        verify(familyRepository, times(1)).findById(1L);
    }

    @Test
    void testGetFamilyById_NotFound() {

        when(familyRepository.findById(1L)).thenReturn(Optional.empty());

        Exception exception = assertThrows(Exception.class, () -> familyController.getFamilyById(1L));
        assertEquals("Family not found with ID 1", exception.getMessage());
        verify(familyRepository, times(1)).findById(1L);
    }

    @Test
    void testCreateFamily() {

        Family newFamily = new Family(null, "Omnivores", new HashSet<>());
        Family savedFamily = new Family(1L, "Omnivores", new HashSet<>());

        when(familyRepository.save(newFamily)).thenReturn(savedFamily);

        ResponseEntity<Family> response = familyController.createFamily(newFamily);

        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        assertEquals(savedFamily, response.getBody());
        verify(familyRepository, times(1)).save(newFamily);
    }

    @Test
    void testUpdateFamily_Found() throws Exception {

        Family updatedFamily = new Family(1L, "Updated Family", new HashSet<>());

        when(familyRepository.existsById(1L)).thenReturn(true);
        when(familyRepository.save(updatedFamily)).thenReturn(updatedFamily);

        ResponseEntity<Family> response = familyController.updateFamily(1L, updatedFamily);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(updatedFamily, response.getBody());
        verify(familyRepository, times(1)).existsById(1L);
        verify(familyRepository, times(1)).save(updatedFamily);
    }

    @Test
    void testUpdateFamily_NotFound() {

        Family updatedFamily = new Family(1L, "Updated Family", new HashSet<>());
        when(familyRepository.existsById(1L)).thenReturn(false);

        Exception exception = assertThrows(Exception.class, () -> familyController.updateFamily(1L, updatedFamily));
        assertEquals("Family not found with ID 1", exception.getMessage());
        verify(familyRepository, times(1)).existsById(1L);
        verify(familyRepository, times(0)).save(updatedFamily);
    }

    @Test
    void testDeleteFamily_Found() throws Exception {

        when(familyRepository.existsById(1L)).thenReturn(true);

        ResponseEntity<Void> response = familyController.deleteFamily(1L);

        assertEquals(HttpStatus.NO_CONTENT, response.getStatusCode());
        verify(familyRepository, times(1)).existsById(1L);
        verify(familyRepository, times(1)).deleteById(1L);
    }

    @Test
    void testDeleteFamily_NotFound() {

        when(familyRepository.existsById(1L)).thenReturn(false);

        Exception exception = assertThrows(Exception.class, () -> familyController.deleteFamily(1L));
        assertEquals("Family not found with ID 1", exception.getMessage());
        verify(familyRepository, times(1)).existsById(1L);
        verify(familyRepository, times(0)).deleteById(1L);
    }

}