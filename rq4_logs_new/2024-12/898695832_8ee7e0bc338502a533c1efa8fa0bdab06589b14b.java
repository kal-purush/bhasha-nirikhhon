package ru.clevertec.carshowroom.controller;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import ru.clevertec.carshowroom.dto.client.ClientRequest;
import ru.clevertec.carshowroom.dto.client.ClientResponse;
import ru.clevertec.carshowroom.dto.client.UpdateClientRequest;
import ru.clevertec.carshowroom.service.CarService;
import ru.clevertec.carshowroom.service.ClientService;

import java.util.List;

@RestController
@Validated
@RequiredArgsConstructor
@RequestMapping("/clients")
public class ClientController {
    private final ClientService clientService;
    private final CarService carService;

    @GetMapping
    @ResponseStatus(HttpStatus.OK)
    public List<ClientResponse> getAllClients() {
        return clientService.findAll();
    }

    @GetMapping("/{id}")
    @ResponseStatus(HttpStatus.OK)
    public ClientResponse getClientById(@PathVariable("id") Long id) { //@NotBlank
        return clientService.findById(id);
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public ClientRequest addClient(@RequestBody @Valid ClientRequest clientRequest) {
        return clientService.addClient(clientRequest);
    }

    @PutMapping()
    @ResponseStatus(HttpStatus.CREATED)
    public UpdateClientRequest update(@RequestBody @Valid UpdateClientRequest updateClientRequest) {
        return clientService.update(updateClientRequest);
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteById(@PathVariable("id") @Valid @NotBlank Long id) {
        clientService.deleteById(id);
    }

    @PatchMapping("/{clientId}/buy-car/{carId}")
    @ResponseStatus(HttpStatus.CREATED)
    public void buyCar(@PathVariable("clientId") Long clientId, @PathVariable("carId") Long carId) {
        clientService.buyCar(clientService.findById(clientId), carService.findById(carId));
    }
}