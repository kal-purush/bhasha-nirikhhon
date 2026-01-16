package com.makeup.demo;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.Rollback;
import org.springframework.transaction.annotation.Transactional;

import static org.junit.jupiter.api.Assertions.*;
@SpringBootTest
class ClientRepositoryTest {

    @Autowired
    private ClientService clientService;

    @Autowired
    private ClientRepository clientRepository;

    @Test
    public void testSaveAndFindClientByUniqueCode() {
        // Given
        ClientEntity client = new ClientEntity();
        client.setUniqueCode("uniqueCode123");
        client.setName("Test Client");

        // When
        clientRepository.save(client);
        ClientEntity foundClient = clientRepository.findClientByUniqueCode("uniqueCode123");

        // Then
        assertNotNull(foundClient);
        assertEquals("Test Client", foundClient.getName());
        assertEquals("uniqueCode123", foundClient.getUniqueCode());
    }

    @Test
    @Transactional
    @Rollback
    public void testDeleteClientByUniqueCode() {
        // Given
        ClientEntity client = new ClientEntity();
        client.setUniqueCode("uniqueCode123");
        client.setName("Test Client");

        // When
        clientRepository.save(client);
        clientRepository.deleteClientByUniqueCode("uniqueCode123");
        ClientEntity deletedClient = clientRepository.findClientByUniqueCode("uniqueCode123");

        // Then
        assertNull(deletedClient);
    }
}
