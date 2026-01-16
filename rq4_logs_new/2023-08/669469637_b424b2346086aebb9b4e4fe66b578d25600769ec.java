package pl.maks.carrental.service.impl;

import org.junit.jupiter.api.Test;
import pl.maks.carrental.controller.productDTO.ClientDTO;
import pl.maks.carrental.convertor.ClientConverter;
import pl.maks.carrental.repository.ClientRepository;
import pl.maks.carrental.repository.model.Client;
import pl.maks.carrental.service.ClientService;
import pl.maks.carrental.validator.ClientValidator;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class ClientServiceImplTest {
    private final ClientRepository clientRepository = mock(ClientRepository.class);
    private final ClientConverter clientConverter = mock(ClientConverter.class);
    private final ClientValidator clientValidator = mock(ClientValidator.class);

    private final ClientService target = new ClientServiceImpl(clientRepository, clientConverter, clientValidator);

    @Test
    void shouldCreateClient() {
        //given
        ClientDTO clientDTO = new ClientDTO();
        Client clientToSave = new Client();
        Client savedClient = new Client(77);

        when(clientConverter.convertToEntity(clientDTO)).thenReturn(clientToSave);
        when(clientRepository.save(clientToSave)).thenReturn(savedClient);

        //when
        Integer actualId = target.createClient(clientDTO);

        //then
        verify(clientValidator).validateClient(clientDTO);
        verify(clientConverter).convertToEntity(clientDTO);

        assertThat(actualId).isEqualTo(savedClient.getId());
    }

    @Test
    void shouldUpdateClient() {
        // given
        ClientDTO updatedClientDTO = new ClientDTO();
        Client updatedClient = new Client();
        Client saveClient = new Client();

        when(clientConverter.convertToEntity(updatedClientDTO)).thenReturn(updatedClient);
        when(clientRepository.findById(anyInt())).thenReturn(Optional.of(saveClient));
        when(clientRepository.save(updatedClient)).thenReturn(updatedClient);
        when(clientConverter.convertToDto(updatedClient)).thenReturn(updatedClientDTO);

        // when
        ClientDTO actualClientDTO = target.updateClient(11 ,updatedClientDTO);

        // then
        verify(clientValidator).validateClient(updatedClientDTO);
        verify(clientRepository).findById(11);
        verify(clientConverter).convertToEntity(updatedClientDTO);
        verify(clientConverter).convertToDto(updatedClient);

        assertThat(actualClientDTO.getId()).isEqualTo(saveClient.getId());
    }

    @Test
    void shouldGetClientById() {
        // given
        Integer clientId = 25;
        Client clientToReturn = new Client(clientId);
        ClientDTO saveClientDTO = new ClientDTO();

        when(clientRepository.findById(clientId)).thenReturn(Optional.of(clientToReturn));
        when(clientConverter.convertToDto(clientToReturn)).thenReturn(saveClientDTO);

        // when
        ClientDTO actualClientDTO = target.getById(clientId);

        // then
        verify(clientRepository).findById(clientId);
        verify(clientConverter).convertToDto(clientToReturn);

        assertThat(actualClientDTO).isEqualTo(saveClientDTO);
    }

    @Test
    void shouldGetAllClients() {
        // given
        List<Client> clients = Arrays.asList(new Client(22), new Client(33));
        List<ClientDTO> saveClientDTOs = Arrays.asList(new ClientDTO(), new ClientDTO());

        when(clientRepository.findAll()).thenReturn(clients);
        when(clientConverter.convertToDto(clients)).thenReturn(saveClientDTOs);

        // when
        List<ClientDTO> actualClientDTOs = target.getAllClients();

        // then
        verify(clientRepository).findAll();
        verify(clientConverter).convertToDto(clients);

        assertThat(actualClientDTOs).isEqualTo(saveClientDTOs);
    }
}
