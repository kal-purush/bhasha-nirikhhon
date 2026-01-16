package ar.edu.unq.tsp.controller;

import ar.edu.unq.tsp.model.Cliente;
import ar.edu.unq.tsp.model.error.NoSeEncontraronLosDatos;
import ar.edu.unq.tsp.service.ClienteService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping(ClienteController.BASE_URL)
public class ClienteController {
    public final static String BASE_URL = "/Cliente";
    private ClienteService clienteService;

    public ClienteController(ClienteService clienteService) {
        this.clienteService = clienteService;
    }

    @GetMapping("/")
    @ResponseStatus(HttpStatus.OK)
    public List<Cliente> getAll() {
        return clienteService.getAll();
    }

    @GetMapping("/{id}")
    @ResponseStatus(HttpStatus.OK)
    public Cliente getById(@PathVariable("id") Long id) {
        Optional<Cliente> unCliente = clienteService.getById(id);
        return unCliente.orElse(new Cliente());
    }

    @PostMapping("/")
    @ResponseStatus(HttpStatus.CREATED)
    public Cliente create(@RequestBody Cliente cliente) {
        return clienteService.create(cliente);
    }

    @PutMapping("/")
    @ResponseStatus(HttpStatus.OK)
    public Cliente update(@RequestBody Cliente cliente) {
        return clienteService.update(cliente);
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.OK)
    public void delete(@PathVariable Long id) {
        Optional<Cliente> unCliente = clienteService.getById(id);
        if(unCliente.isPresent()) {
            clienteService.delete(unCliente.get());
        }
    }

}