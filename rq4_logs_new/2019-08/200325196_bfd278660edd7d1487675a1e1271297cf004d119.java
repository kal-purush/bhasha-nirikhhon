package co.edu.eam.ingesoft.stores.controllers;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import co.edu.eam.ingesoft.stores.model.Person;
import co.edu.eam.ingesoft.stores.services.PersonService;

/**
 * REst controller for person entity.
 *
 * @author caferrerb
 *
 */
@RestController
@RequestMapping("api/stores-ms/person")
public class PersonController {

  /**
   * person service.
   */
  @Autowired
  private PersonService personService;

  /**
   * create a person operation.
   *
   * @param person person to create
   */
  @PostMapping(value = "/")
  public void create(@RequestBody Person person) {
    personService.create(person);
  }

  /**
   * find a person.
   *
   * @param id id for person to find
   * @return person with id
   */
  @GetMapping(value = "/{id}")
  public Person find(@PathVariable Integer id) {
    return personService.find(id);
  }

  /**
   * Delete a person.
   *
   * @param id id person to delete
   */
  @DeleteMapping(value = "/{id}")
  public void delete(@PathVariable Integer id) {
    personService.delete(id);
  }

  /**
   * Edit a person.
   *
   * @param person perso to edit
   */
  @PutMapping(value = "/")
  public void edit(@RequestBody Person person) {
    personService.create(person);
  }

  /**
   * find a person by name.
   *
   * @param name name person to find
   * @return list of person with a name
   */
  @GetMapping(value = "/find_by_name")
  public List<Person> findByName(@RequestParam String name) {
    System.out.println(name);
    return personService.findByName(name);
  }

  /**
   * list all persons.
   *
   * @return list of all persons
   */
  @GetMapping(value = "/all")
  public List<Person> findAll() {
    return personService.listAll();
  }
}