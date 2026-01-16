package ru.romankuznetsov.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.context.annotation.SessionScope;
import ru.romankuznetsov.dto.PersonDto;
import ru.romankuznetsov.entity.Person;
import ru.romankuznetsov.exceptions.PersonNotFoundException;
import ru.romankuznetsov.repository.CityRepository;
import ru.romankuznetsov.repository.PersonRepository;

import java.util.List;
import java.util.NoSuchElementException;

@RestController
@SessionScope
@RequestMapping("api/v1/person")
public class PersonController {

    @Autowired
    private PersonRepository personRepository;
    @Autowired
    private CityRepository cityRepository;

    @GetMapping
    public List<Person> getPeople(){
        return personRepository.findAll();
    }

    @GetMapping("{id}")
    public Person findPerson(@PathVariable long id){
        return personRepository.findById(id)
                .orElseThrow(() -> new PersonNotFoundException(
                        String.format("пользователь c ID{%s} не найден", id)
        ));
    }

//    @PostMapping
//    public void savePerson(@RequestBody Person person){
//        personRepository.save(person);
//    }

    @PostMapping
    public Person savePerson(@RequestBody PersonDto personDto){
        Person person = new Person();
        person.setFirstName(personDto.getFirstName());
        person.setLastName(personDto.getLastName());
        if (personDto.getCityId() != null){
            person.setCity(cityRepository.findById(personDto.getCityId())
                    .orElseThrow(() -> new PersonNotFoundException(
                            String.format("пользователь c ID{%s} не найден", personDto.getCityId())
            )));
        }
        personRepository.save(person);
        return person;
    }

    @PutMapping
    public void updatePerson(@RequestBody Person person){
        personRepository.save(person);
    }

    @DeleteMapping("{id}")
    public void deletePerson(@PathVariable long id){
        personRepository.delete(personRepository.findById(id)
                .orElseThrow(() -> new PersonNotFoundException(
                        String.format("пользователь c ID{%s} не найден", id))));
    }
}