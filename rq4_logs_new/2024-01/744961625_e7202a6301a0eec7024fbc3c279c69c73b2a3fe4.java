package uz.pdp.simline.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import uz.pdp.simline.dto.request.CustomerLoginDto;
import uz.pdp.simline.dto.request.CustomerRegisterDto;
import uz.pdp.simline.repository.CustomerRepository;
import uz.pdp.simline.service.CustomerService;

@RestController
@RequestMapping("/auth")
@RequiredArgsConstructor
public class AuthController {
    private final CustomerService customerService;
    @PostMapping("/customer/register")
    public ResponseEntity<?> customerRegister(@RequestBody CustomerRegisterDto customerRegisterDto){
        return ResponseEntity.ok(customerService.register(customerRegisterDto));
    }
    @PostMapping("/customer/login")
    public ResponseEntity<?> customerLogin(@RequestBody CustomerLoginDto customerLoginDto){
        return ResponseEntity.ok(customerService.login(customerLoginDto));
    }
    //TODO add employee register and login

}