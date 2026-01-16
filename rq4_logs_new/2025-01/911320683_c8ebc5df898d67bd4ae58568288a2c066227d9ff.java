package com.sofkau.bank.controllers;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.sofkau.bank.entities.Client;
import com.sofkau.bank.http.requests.CreateClientRequest;
import com.sofkau.bank.http.responses.ClientResponse;
import com.sofkau.bank.services.clients.ClientService;

@RestController
@RequestMapping("/clients")
public class ClientController {
    private ClientService clientService;

    @PostMapping
    public ClientResponse createClient(CreateClientRequest clientRequest) {
        Client client = clientRequest.toClient();
        return ClientResponse.from(clientService.createClient(client));
    }
}