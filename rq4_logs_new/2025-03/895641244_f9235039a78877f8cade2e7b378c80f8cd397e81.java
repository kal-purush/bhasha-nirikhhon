package ru.innopolis.attestations.attestation01.repositories.impl;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.innopolis.attestations.attestation01.UserFieldsLinePositionsEnum;
import ru.innopolis.attestations.attestation01.exceptions.UserNotFoundException;
import ru.innopolis.attestations.attestation01.models.User;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class UsersRepositoryFileImplTest {

    UsersRepositoryFileImpl repo;

    @BeforeEach
    void setUp() {
        repo = new UsersRepositoryFileImpl();
        repo.deleteAll();
    }

    @AfterEach
    void tearDown() {
        repo.deleteAll();
    }

    static User createUser() {
        var user = new User();
        var id = "f5a8a3cb-4ac9-4b3b-8a65-c424e129b9d2";
        user.setId(id);
        user.setCreatedAt(LocalDateTime.now());
        user.setLogin("noisemc_99");
        var password = "789ghs_";
        user.setPasswords(password, password);
        user.setLastName("Крылов");
        user.setFirstName("Виктор");
        user.setMiddleName("Павлович");
        user.setAge(25);
        user.setWorker(true);
        return user;
    }

    @Test
    void create() throws IOException {
        User user = createUser();
        repo.create(user);
        try (var reader = Files.newBufferedReader(Paths.get(UsersRepositoryFileImpl.FILE_PATH))) {
            var line = reader.readLine();
            assertEquals(user.getId(), line.split("\\|")[UserFieldsLinePositionsEnum.ID.getIndex()]);
        }
        assertNotNull(repo.findById(user.getId()));
    }

    @Test
    void findById() {
        User user = createUser();
        repo.create(user);
        assertNotNull(repo.findById(user.getId()));
    }

    @Test
    void userNotFoundById() {
        var id = UUID.randomUUID().toString();
        assertThrows(UserNotFoundException.class, () -> repo.findById(id));
    }

    @Test
    void findAll() {
        var user = createUser();
        repo.create(user);
        List<User> users = repo.findAll();
        assertEquals(1, users.size());
        assertEquals(user, users.getFirst());
    }

    @Test
    void updateExisting() throws IOException {
        var user = createUser();
        repo.create(user);
        int newAge = user.getAge() + 1;
        user.setAge(newAge);
        repo.update(user);
        try (var reader = Files.newBufferedReader(Paths.get(UsersRepositoryFileImpl.FILE_PATH))) {
            var line = reader.readLine();
            assertEquals(user.getAge(), Integer.parseInt(line.split("\\|")[UserFieldsLinePositionsEnum.AGE.getIndex()]));
        }
    }

    @Test
    void createWhenUpdating() throws IOException {
        var user = createUser();
        repo.update(user);
        try (var reader = Files.newBufferedReader(Paths.get(UsersRepositoryFileImpl.FILE_PATH))) {
            var line = reader.readLine();
            assertEquals(user.getId(), line.split("\\|")[UserFieldsLinePositionsEnum.ID.getIndex()]);
        }
    }

    @Test
    void deleteById() throws IOException {
        var user = createUser();
        repo.create(user);
        repo.deleteById(user.getId());
        assertThrows(UserNotFoundException.class, () -> repo.findById(user.getId()));
        assertFileEmpty();
    }

    @Test
    void deleteAll() throws IOException {
        var user = createUser();
        repo.create(user);
        repo.deleteAll();
        assertFileEmpty();
    }

    void assertFileEmpty() throws IOException {
        try (var reader = Files.newBufferedReader(Paths.get(UsersRepositoryFileImpl.FILE_PATH))) {
            assertEquals(-1, reader.read());
        }
    }

}