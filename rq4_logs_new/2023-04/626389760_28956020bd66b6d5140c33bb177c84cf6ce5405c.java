package com.it.academy.mortgage.calculator.controllers;

import com.it.academy.mortgage.calculator.models.Customer;
import com.it.academy.mortgage.calculator.repos.CustomerRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Slf4j
@RestController
@RequestMapping("customers")
@RequiredArgsConstructor
@CrossOrigin(origins = "http://localhost:4200")
public class CustomerController {
    @Autowired
    CustomerRepository repository;
    @GetMapping()
    public List<Customer> all(){
        return repository.findAll();
    }

    @PostMapping
    public void save (@RequestBody Customer student){repository.save(student);
    }

    @DeleteMapping("/{id}")
    public void deleteStudent(@PathVariable final Integer id){
        repository.deleteById(id);
    }

    @GetMapping("/byname")
    public List<Customer>  getByName(@RequestParam() String name) {return  repository.getByName(name);}
}