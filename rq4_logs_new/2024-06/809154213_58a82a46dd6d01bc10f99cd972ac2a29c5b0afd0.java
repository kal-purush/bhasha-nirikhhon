package de.ait_tr.g_40_shop.controller;

import de.ait_tr.g_40_shop.domain.entity.Customer;
import de.ait_tr.g_40_shop.service.interfaces.CustomerService;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/customers")
public class CustomerController {

    private CustomerService service;

    public CustomerController(CustomerService service) {
        this.service = service;
    }

    @PostMapping
    public Customer save(@RequestBody Customer customer) {
        return service.save(customer);
    }

    @GetMapping
    public List<Customer> get(@RequestParam(required = false) Long id) {
        if (id == null) {
            return service.getAllActiveCustomers();
        } else {
            return List.of(service.getById(id));
        }
    }

    @PutMapping
    public Customer update(@RequestBody Customer customer) {
        return service.update(customer);
    }

    @DeleteMapping
    public void delete(@RequestParam Long id) {
        service.deleteById(id);
    }

    @DeleteMapping("/byName")
    public void delete(@RequestParam String name) {
        service.deleteByName(name);
    }

    @PostMapping("/restore")
    public void restore(@RequestParam Long id){
        service.restoreById(id);
    }
    @GetMapping("/quantity")
    public long getQuantity() {
        return service.getActiveCustomersQuantity();
    }
}