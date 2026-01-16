package dev.forkingaround.zootopia.controllers;

import dev.forkingaround.zootopia.models.Type;
import dev.forkingaround.zootopia.repositories.TypeRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class TypeControllerTest {

    @Mock
    private TypeRepository typeRepository;

    @InjectMocks
    private TypeController typeController;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testGetAllTypes() {

        Type type1 = Type.builder().id(1L).name("Type1").build();
        Type type2 = Type.builder().id(2L).name("Type2").build();
        List<Type> types = List.of(type1, type2);

        when(typeRepository.findAll()).thenReturn(types);

        ResponseEntity<List<Type>> response = typeController.getAllTypes();

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(types, response.getBody());
        verify(typeRepository, times(1)).findAll();
    }

    @Test
    void testGetTypeById_Found() throws Exception {

        Type type = Type.builder().id(1L).name("Type1").build();
        when(typeRepository.findById(1L)).thenReturn(Optional.of(type));

        ResponseEntity<Type> response = typeController.getTypeById(1L);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(type, response.getBody());
        verify(typeRepository, times(1)).findById(1L);
    }

    @Test
    void testGetTypeById_NotFound() {

        when(typeRepository.findById(1L)).thenReturn(Optional.empty());

        Exception exception = assertThrows(Exception.class, () -> typeController.getTypeById(1L));
        assertEquals("Type not found with ID 1", exception.getMessage());
        verify(typeRepository, times(1)).findById(1L);
    }

    @Test
    void testCreateType() {

        Type newType = Type.builder().name("NewType").build();
        Type createdType = Type.builder().id(1L).name("NewType").build();

        when(typeRepository.save(newType)).thenReturn(createdType);

        ResponseEntity<Type> response = typeController.createType(newType);

        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        assertEquals(createdType, response.getBody());
        verify(typeRepository, times(1)).save(newType);
    }

    @Test
    void testUpdateType_Found() throws Exception {

        Type updatedType = Type.builder().id(1L).name("UpdatedType").build();
        when(typeRepository.existsById(1L)).thenReturn(true);
        when(typeRepository.save(updatedType)).thenReturn(updatedType);

        ResponseEntity<Type> response = typeController.updateType(1L, updatedType);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(updatedType, response.getBody());
        verify(typeRepository, times(1)).existsById(1L);
        verify(typeRepository, times(1)).save(updatedType);
    }

    @Test
    void testUpdateType_NotFound() {

        Type updatedType = Type.builder().id(1L).name("UpdatedType").build();
        when(typeRepository.existsById(1L)).thenReturn(false);

        Exception exception = assertThrows(Exception.class, () -> typeController.updateType(1L, updatedType));
        assertEquals("Type not found with ID 1", exception.getMessage());
        verify(typeRepository, times(1)).existsById(1L);
        verify(typeRepository, times(0)).save(updatedType);
    }

    @Test
    void testDeleteType_Found() throws Exception {

        when(typeRepository.existsById(1L)).thenReturn(true);

        ResponseEntity<Void> response = typeController.deleteType(1L);

        assertEquals(HttpStatus.NO_CONTENT, response.getStatusCode());
        verify(typeRepository, times(1)).existsById(1L);
        verify(typeRepository, times(1)).deleteById(1L);
    }

    @Test
    void testDeleteType_NotFound() {

        when(typeRepository.existsById(1L)).thenReturn(false);

        Exception exception = assertThrows(Exception.class, () -> typeController.deleteType(1L));
        assertEquals("Type not found with ID 1", exception.getMessage());
        verify(typeRepository, times(1)).existsById(1L);
        verify(typeRepository, times(0)).deleteById(1L);
    }

}