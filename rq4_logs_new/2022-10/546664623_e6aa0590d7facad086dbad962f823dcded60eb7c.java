package liga.medical.personservice.core.controller;

import liga.medical.personservice.core.model.Address;
import liga.medical.personservice.core.service.AddressService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
public class AddressController {
    private AddressService addressService;

    @Autowired
    public AddressController(AddressService addressService) {
        this.addressService = addressService;
    }

    @GetMapping(path = "/address", produces = "application/json")
    public List<Address> getAddress() {
        return addressService.getAddress();
    }

    @GetMapping(path = "/address/{id}", produces = "application/json")
    public Address getAddress(@PathVariable("id") long id) {
        return addressService.getAddressById(id);
    }

    @PostMapping
    public void addAddress(@RequestBody Address address) {
        addressService.addAddress(address);
    }
}