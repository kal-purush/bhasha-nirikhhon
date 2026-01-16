package com.management.restaurant.controllers;

import com.management.restaurant.DTO.ClientRequestDTO;
import com.management.restaurant.DTO.ClientResponseDTO;
import com.management.restaurant.models.client.Client;
import com.management.restaurant.services.ClientService;
import com.management.restaurant.utils.DtoConverter;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/cliente")
public class ClientController {

  private ClientService service;

  @Autowired
  public ClientController(ClientService service) {
    this.service = service;
  }

  @PostMapping()
  public ResponseEntity<String> addClient(@Validated @RequestBody ClientRequestDTO clientRequestDto) {
    Client client = new Client(
      clientRequestDto.getName(),
      clientRequestDto.getEmail(),
      clientRequestDto.getNumberPhone()
    );
    service.addClient(client);
    return ResponseEntity.ok("Cliente agregado exitosamente.");
  }
  @GetMapping
  public ResponseEntity<List<ClientResponseDTO>> listClient(){
    List<Client> clients = service.listClient();
    List<ClientResponseDTO> response = clients.stream()
      .map(DtoConverter::convertToResponseDTO)
      .collect(Collectors.toList());
    return ResponseEntity.ok(response);
  }
 /* @GetMapping("/${id}")
  public Optional<Client> showClient(Long id){
   return service.showClientById(id);
}
*/
}