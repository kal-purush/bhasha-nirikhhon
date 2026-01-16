package main.controllers;

import main.modules.ApplicationEntity;
import main.modules.dtos.ApplicationEntityDTO;
import main.repositories.EntityRepository;
import main.services.EntityService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class EntityControllerTests {

    @Mock
    private EntityService entityService;

    @InjectMocks
    private EntityController entityController;

    private ApplicationEntity entity;

    @BeforeEach
    void setUp(){
        entity = new ApplicationEntity(new ApplicationEntityDTO("Test Name", "Test Description"));
    }

    @Test
    void testEntities(){
        when(entityService.getEntities()).thenReturn(Collections.singletonList(entity));

        List<ApplicationEntity> entities = entityController.getAllEntities();

        assertEquals(1, entities.size());
        assertEquals("Test Name", entities.getFirst().getName());
        assertEquals("Test Description", entities.getFirst().getDescription());
        verify(entityService, times(1)).getEntities();
    }

    @Test
    void testCreateEntity() {
        when(entityService.createEntity(ArgumentMatchers.any(ApplicationEntity.class))).thenReturn(entity);

        ApplicationEntity createdEntity = entityController.addEntity(new ApplicationEntityDTO("Test Name", "Test Description"));

        assertEquals("Test Name", createdEntity.getName());
        assertEquals("Test Description", createdEntity.getDescription());
        verify(entityService, times(1)).createEntity(ArgumentMatchers.any(ApplicationEntity.class));
    }

}