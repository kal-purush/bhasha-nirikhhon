package tech.munidigital.lavadero.service.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.server.ResponseStatusException;
import tech.munidigital.lavadero.dto.request.ClienteRequestDTO;
import tech.munidigital.lavadero.dto.response.ClienteResponseDTO;
import tech.munidigital.lavadero.entity.Cliente;
import tech.munidigital.lavadero.mappers.ClienteMapper;
import tech.munidigital.lavadero.repository.ClienteRepository;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ClienteServiceImplTest {

    @Mock
    private ClienteRepository clienteRepository;

    @Mock
    private ClienteMapper clienteMapper;

    @InjectMocks
    private ClienteServiceImpl clienteService;

    private ClienteRequestDTO clienteRequestDTO;
    private Cliente cliente;
    private ClienteResponseDTO clienteResponseDTO;

    @BeforeEach
    void setUp() {
        // Configuramos un ClienteRequestDTO válido
        clienteRequestDTO = new ClienteRequestDTO();
        clienteRequestDTO.setNombre("Juan Pérez");
        clienteRequestDTO.setCorreoElectronico("juan.perez@example.com");
        clienteRequestDTO.setTelefono("123456789");

        // Configuramos la entidad Cliente correspondiente
        cliente = new Cliente();
        cliente.setId(1L);
        cliente.setNombre(clienteRequestDTO.getNombre());
        cliente.setCorreoElectronico(clienteRequestDTO.getCorreoElectronico());
        cliente.setTelefono(clienteRequestDTO.getTelefono());

        // Configuramos el DTO de respuesta
        clienteResponseDTO = new ClienteResponseDTO();
        clienteResponseDTO.setId(1L);
        clienteResponseDTO.setNombre(cliente.getNombre());
        clienteResponseDTO.setCorreoElectronico(cliente.getCorreoElectronico());
        clienteResponseDTO.setTelefono(cliente.getTelefono());
        clienteResponseDTO.setVehiculos(null); // o new ArrayList<>()
    }

    @Test
    void createCliente_validRequest_returnsClienteResponseDTO() {
        // Simulamos que no existe un cliente con el mismo correo
        when(clienteRepository.findByCorreoElectronico(clienteRequestDTO.getCorreoElectronico()))
                .thenReturn(Optional.empty());
        // Simulamos el mapeo de DTO a entidad
        when(clienteMapper.toEntity(clienteRequestDTO)).thenReturn(cliente);
        // Simulamos el guardado del cliente
        when(clienteRepository.save(cliente)).thenReturn(cliente);
        // Simulamos el mapeo de entidad a DTO de respuesta
        when(clienteMapper.toDto(cliente)).thenReturn(clienteResponseDTO);

        // Ejecutamos el servicio
        ClienteResponseDTO result = clienteService.createCliente(clienteRequestDTO);

        // Verificamos los resultados
        assertNotNull(result);
        assertEquals(1L, result.getId());
        assertEquals("Juan Pérez", result.getNombre());
        verify(clienteRepository).findByCorreoElectronico(clienteRequestDTO.getCorreoElectronico());
        verify(clienteRepository).save(cliente);
    }

    @Test
    void createCliente_duplicateEmail_throwsException() {
        // Simulamos que ya existe un cliente con el correo electrónico
        when(clienteRepository.findByCorreoElectronico(clienteRequestDTO.getCorreoElectronico()))
                .thenReturn(Optional.of(cliente));

        // Verificamos que se lance la excepción por correo duplicado
        ResponseStatusException exception = assertThrows(ResponseStatusException.class, () -> {
            clienteService.createCliente(clienteRequestDTO);
        });
        assertTrue(exception.getReason().contains("El correo electrónico ya está registrado"));
        verify(clienteRepository).findByCorreoElectronico(clienteRequestDTO.getCorreoElectronico());
        verify(clienteRepository, never()).save(any(Cliente.class));
    }

    @Test
    void getClienteById_existingClient_returnsClienteResponseDTO() {
        // Simulamos que se encuentra el cliente
        when(clienteRepository.findById(1L)).thenReturn(Optional.of(cliente));
        when(clienteMapper.toDto(cliente)).thenReturn(clienteResponseDTO);

        ClienteResponseDTO result = clienteService.getClienteById(1L);
        assertNotNull(result);
        assertEquals(1L, result.getId());
        verify(clienteRepository).findById(1L);
    }

    @Test
    void getClienteById_clientNotFound_throwsException() {
        // Simulamos que no se encuentra el cliente
        when(clienteRepository.findById(1L)).thenReturn(Optional.empty());

        ResponseStatusException exception = assertThrows(ResponseStatusException.class, () -> {
            clienteService.getClienteById(1L);
        });
        assertTrue(exception.getReason().contains("Cliente no encontrado con id: 1"));
        verify(clienteRepository).findById(1L);
    }

    @Test
    void getAllClientes_returnsListOfClienteResponseDTO() {
        // Simulamos que el repositorio retorna una lista con un cliente
        when(clienteRepository.findAll()).thenReturn(Arrays.asList(cliente));
        when(clienteMapper.toDto(cliente)).thenReturn(clienteResponseDTO);

        List<ClienteResponseDTO> result = clienteService.getAllClientes();
        assertNotNull(result);
        assertEquals(1, result.size());
        verify(clienteRepository).findAll();
    }

}