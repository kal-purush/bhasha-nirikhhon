package com.example.auth_service.controller;

import com.example.auth_service.model.ObjectEntity;
import com.example.auth_service.model.ObjectType;
import com.example.auth_service.service.ObjectService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ObjectControllerTest {

    @Mock
    private ObjectService objectService;

    @InjectMocks
    private ObjectController objectController;

    private ObjectEntity createTestObject(Long id, String name, ObjectType objectType, ObjectEntity parent) {
        return ObjectEntity.builder()
                .id(id)
                .name(name)
                .objectType(objectType)
                .parent(parent)
                .createdAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .build();
    }

    @Test
    @DisplayName("GET /real-estate-objects - Получение всех объектов (успешный сценарий)")
    void getAllObjects_ShouldReturnAllObjects() {
        // Подготовка тестовых данных
        ObjectEntity project = createTestObject(1L, "ЖК Ромашка", ObjectType.PROJECT, null);
        ObjectEntity building = createTestObject(2L, "Корпус 1", ObjectType.BUILDING, project);
        List<ObjectEntity> expectedObjects = Arrays.asList(project, building);

        when(objectService.getAllObjects()).thenReturn(expectedObjects);

        // Выполнение запроса
        ResponseEntity<List<ObjectEntity>> response = objectController.getAllObjects();

        // Проверки
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(2, response.getBody().size());
        assertEquals("ЖК Ромашка", response.getBody().get(0).getName());
        verify(objectService, times(1)).getAllObjects();
    }

    @Test
    @DisplayName("GET /real-estate-objects/{id} - Получение существующего объекта (успешный сценарий)")
    void getObjectById_WhenExists_ShouldReturnObject() {
        // Подготовка тестовых данных
        Long id = 1L;
        ObjectEntity project = createTestObject(id, "ЖК Ромашка", ObjectType.PROJECT, null);

        when(objectService.getObjectById(id)).thenReturn(Optional.of(project));

        // Выполнение запроса
        ResponseEntity<ObjectEntity> response = objectController.getObjectsById(id);

        // Проверки
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals("ЖК Ромашка", response.getBody().getName());
        verify(objectService, times(1)).getObjectById(id);
    }

    @Test
    @DisplayName("GET /real-estate-objects/{id} - Попытка получить несуществующий объект (негативный сценарий)")
    void getObjectById_WhenNotExists_ShouldReturnNotFound() {
        // Подготовка тестовых данных
        Long id = 999L;

        when(objectService.getObjectById(id)).thenReturn(Optional.empty());

        // Выполнение запроса
        ResponseEntity<ObjectEntity> response = objectController.getObjectsById(id);

        // Проверки
        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
        verify(objectService, times(1)).getObjectById(id);
    }

    @Test
    @DisplayName("GET /real-estate-objects/by-type - Получение объектов по типу (успешный сценарий)")
    void getObjectsByType_ShouldReturnFilteredObjects() {
        // Подготовка тестовых данных
        ObjectType type = ObjectType.APARTMENT;
        ObjectEntity apartment1 = createTestObject(1L, "Квартира 101", ObjectType.APARTMENT, null);
        ObjectEntity apartment2 = createTestObject(2L, "Квартира 102", ObjectType.APARTMENT, null);
        List<ObjectEntity> expectedApartments = Arrays.asList(apartment1, apartment2);

        when(objectService.getObjectsByType(type)).thenReturn(expectedApartments);

        // Выполнение запроса
        ResponseEntity<List<ObjectEntity>> response = objectController.getObjectsByType(type);

        // Проверки
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(2, response.getBody().size());
        assertTrue(response.getBody().stream().allMatch(o -> o.getObjectType() == ObjectType.APARTMENT));
        verify(objectService, times(1)).getObjectsByType(type);
    }

    @Test
    @DisplayName("GET /real-estate-objects/{id}/children - Получение дочерних объектов (успешный сценарий)")
    void getChildren_ShouldReturnChildObjects() {
        // Подготовка тестовых данных
        Long parentId = 1L;
        ObjectEntity parent = createTestObject(parentId, "ЖК Ромашка", ObjectType.PROJECT, null);
        ObjectEntity child1 = createTestObject(2L, "Корпус 1", ObjectType.BUILDING, parent);
        ObjectEntity child2 = createTestObject(3L, "Корпус 2", ObjectType.BUILDING, parent);
        List<ObjectEntity> expectedChildren = Arrays.asList(child1, child2);

        when(objectService.getChildren(parentId)).thenReturn(expectedChildren);

        // Выполнение запроса
        ResponseEntity<List<ObjectEntity>> response = objectController.getChildren(parentId);

        // Проверки
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(2, response.getBody().size());
        assertTrue(response.getBody().stream().allMatch(o -> o.getParent().getId().equals(parentId)));
        verify(objectService, times(1)).getChildren(parentId);
    }

    @Test
    @DisplayName("POST /real-estate-objects - Создание нового объекта (успешный сценарий)")
    void createObject_ShouldCreateNewObject() {
        // Подготовка тестовых данных
        ObjectEntity newObject = createTestObject(null, "Новый проект", ObjectType.PROJECT, null);
        ObjectEntity savedObject = createTestObject(1L, "Новый проект", ObjectType.PROJECT, null);

        when(objectService.createObject(newObject)).thenReturn(savedObject);

        // Выполнение запроса
        ResponseEntity<ObjectEntity> response = objectController.createObject(newObject);

        // Проверки
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody().getId());
        assertEquals("Новый проект", response.getBody().getName());
        verify(objectService, times(1)).createObject(newObject);
    }

    @Test
    @DisplayName("PUT /real-estate-objects/{id} - Обновление существующего объекта (успешный сценарий)")
    void updateObject_ShouldUpdateExistingObject() {
        // Подготовка тестовых данных
        Long id = 1L;
        ObjectEntity existingObject = createTestObject(id, "Старое название", ObjectType.PROJECT, null);
        ObjectEntity updatedData = createTestObject(null, "Новое название", ObjectType.PROJECT, null);
        ObjectEntity updatedObject = createTestObject(id, "Новое название", ObjectType.PROJECT, null);

        when(objectService.updateObject(id, updatedData)).thenReturn(updatedObject);

        // Выполнение запроса
        ResponseEntity<ObjectEntity> response = objectController.updateObject(id, updatedData);

        // Проверки
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(id, response.getBody().getId());
        assertEquals("Новое название", response.getBody().getName());
        verify(objectService, times(1)).updateObject(id, updatedData);
    }

    @Test
    @DisplayName("DELETE /real-estate-objects/{id} - Удаление объекта (успешный сценарий)")
    void deleteObject_ShouldDeleteObject() {
        // Подготовка тестовых данных
        Long id = 1L;
        doNothing().when(objectService).deleteObject(id);

        // Выполнение запроса
        ResponseEntity<Void> response = objectController.deleteObject(id);

        // Проверки
        assertEquals(HttpStatus.NO_CONTENT, response.getStatusCode());
        verify(objectService, times(1)).deleteObject(id);
    }
}