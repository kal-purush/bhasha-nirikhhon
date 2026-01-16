package com.example.demo.api;

import com.example.demo.model.Person;
import com.example.demo.service.PersonService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.NonNull;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import java.util.List;
import java.util.UUID;

@RequestMapping("api/v1/person")
@RestController
public class PersonController {

    private final PersonService personService;
    private UUID id;
    private @Valid boolean clockIn;

    @Autowired
    public PersonController(PersonService personService)
    {
        this.personService = personService;
    }


    @PostMapping
    public void addPerson(@Valid @NonNull @RequestBody Person person){
        personService.addPerson(person);
    }

    @GetMapping
    public List<Person> getAllPeople(){
        return personService.getAllPeople();
    }

    @GetMapping(path = "{id}")
    public Person getPersonById(@PathVariable("id") UUID id){
        return personService.getPersonById(id).orElse(null);
    }

    @GetMapping(path = "{id}"+"/myClockTime")
    public List<String> selectMyClockTimeByID(@PathVariable("id") UUID id){
        return personService.selectMyClockTimeByID(id);
    }

    @GetMapping(path = "{id}"+"/sleepTimeTotal")
    public String getMySleepTimeTotal(@PathVariable("id") UUID id){
        return personService.getMySleepTimeTotal(id);
    }

    @DeleteMapping(path = "{id}")
    public void deletePersonById(@PathVariable("id") UUID id){
        personService.deletePerson(id);
    }


    @PutMapping(path = "{id}")
    public void updatePerson(@PathVariable("id") UUID id, @Valid @NonNull @RequestBody Person personToUpdate){
        personService.updatePerson(id, personToUpdate);
    }

    @PutMapping(path = "{id}"+"/myClockTime")
    public void addClockInTime(@PathVariable("id") UUID id, boolean clockIn){
        this.id = id;
        this.clockIn = clockIn;
        personService.addClockInTime(id,clockIn);
    }

    /*@PutMapping(path = "{id}"+"/sleepTimeTotal")
    public void setSleepTimeTotal(@PathVariable("id") UUID id) {
        this.id = id;
        personService.setSleepTimeTotal(id);
    }*/


    }