package wolox.training.controllers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import wolox.training.exception.UsersNotFoundException;
import wolox.training.models.Users;
import wolox.training.repositories.UsersRepository;

@RestController
@RequestMapping("/api/users")
public class UsersController {

    @Autowired
    private UsersRepository usersRepository;

    @GetMapping
    @ResponseStatus(HttpStatus.OK)
    public Iterable findAll() {
        return usersRepository.findAll();
    }

    @GetMapping("/{id}")
    @ResponseStatus(HttpStatus.OK)
    public Users findById(@PathVariable Long id) {
        return usersRepository.findById(id).orElseThrow(UsersNotFoundException::new);
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Users create(@RequestBody Users users) {
        return usersRepository.save(users);
    }

    @PutMapping("/{id}")
    @ResponseStatus(HttpStatus.CREATED)
    public Users update(@RequestBody Users users, @PathVariable Long id) {
        usersRepository.findById(id).orElseThrow(UsersNotFoundException::new);
        return usersRepository.save(users);
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void delete(@PathVariable Long id) {
        usersRepository.findById(id).orElseThrow(UsersNotFoundException::new);
        usersRepository.deleteById(id);
    }
}