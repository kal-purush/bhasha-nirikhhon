package com.sofkau.bank.controllers;

import com.sofkau.bank.entities.Client;
import com.sofkau.bank.http.requests.CreateClientRequest;
import com.sofkau.bank.http.responses.ClientResponse;
import com.sofkau.bank.services.clients.ClientService;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/users")
public class ClientController {
    private final ClientService clientService;

    public ClientController(ClientService clientService) {
        this.clientService = clientService;
    }

    @GetMapping
    public ClientResponse getClient(@AuthenticationPrincipal Client client) {
        return ClientResponse.from(client);
    }

    @PutMapping
    public ClientResponse updateClient(
            @RequestBody CreateClientRequest clientRequest,
            @AuthenticationPrincipal Client client) {
        String email = client.getUsername();
        Client toUpdate = clientRequest.toClient();

        return ClientResponse.from(clientService.updateClient(email, toUpdate));
    }

    @DeleteMapping
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteClient(
            @AuthenticationPrincipal Client client) {
        clientService.deleteClientByEmail(client.getUsername());
    }
}