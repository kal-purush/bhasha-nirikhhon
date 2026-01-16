package org.uresti.pozarreal.service.impl;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.uresti.pozarreal.dto.User;
import org.uresti.pozarreal.model.Login;
import org.uresti.pozarreal.model.Representative;
import org.uresti.pozarreal.model.RoleByUser;
import org.uresti.pozarreal.repository.LoginRepository;
import org.uresti.pozarreal.repository.RepresentativeRepository;
import org.uresti.pozarreal.repository.RolesRepository;
import org.uresti.pozarreal.repository.UserRepository;

import java.util.*;

public class UserServiceImplTests {

    @Test
    public void givenUserId_whenRemoveRole_thenRemoveRoleForUserFromAList() {
        // Given:
        RolesRepository rolesRepository = Mockito.mock(RolesRepository.class);
        UserRepository userRepository = Mockito.mock(UserRepository.class);
        RepresentativeRepository representativeRepository = Mockito.mock(RepresentativeRepository.class);
        LoginRepository loginRepository = Mockito.mock(LoginRepository.class);

        UserServiceImpl userService = new UserServiceImpl(
                rolesRepository,
                userRepository,
                representativeRepository,
                loginRepository);

        List<String> roles = new LinkedList<>(Arrays.asList(
                "ROLE_REPRESENTATIVE",
                "ROLE_RESIDENT",
                "ROLE_USER_MANAGER",
                "ROLE_CHIPS_UPDATER",
                "ROLE_SCHOOL_MANAGER"));

        Mockito.when(rolesRepository.findRolesByUser("userId")).thenReturn(roles);

        // When:
        List<String> rolesByUser = userService.removeRole("userId", "ROLE_ADMIN");

        // Then:
        Assertions.assertThat(rolesByUser).isNotNull();
        Assertions.assertThat(rolesByUser.size()).isEqualTo(5);
        Assertions.assertThat(rolesByUser.get(0)).isEqualTo("ROLE_REPRESENTATIVE");
        Assertions.assertThat(rolesByUser.get(1)).isEqualTo("ROLE_RESIDENT");
        Assertions.assertThat(rolesByUser.get(2)).isEqualTo("ROLE_USER_MANAGER");
        Assertions.assertThat(rolesByUser.get(3)).isEqualTo("ROLE_CHIPS_UPDATER");
        Assertions.assertThat(rolesByUser.get(4)).isEqualTo("ROLE_SCHOOL_MANAGER");

        Mockito.verify(rolesRepository).deleteRoleByUserIdAndRole("userId", "ROLE_ADMIN");
    }

    @Test
    public void givenUserId_whenAddRole_thenAddNewRoleToUser() {
        // Given:
        RolesRepository rolesRepository = Mockito.mock(RolesRepository.class);
        UserRepository userRepository = Mockito.mock(UserRepository.class);
        RepresentativeRepository representativeRepository = Mockito.mock(RepresentativeRepository.class);
        LoginRepository loginRepository = Mockito.mock(LoginRepository.class);

        ArgumentCaptor<RoleByUser> argumentCaptor = ArgumentCaptor.forClass(RoleByUser.class);

        UserServiceImpl userService = new UserServiceImpl(
                rolesRepository,
                userRepository,
                representativeRepository,
                loginRepository);

        RoleByUser roleByUser = RoleByUser.builder()
                .id("id")
                .userId("userId")
                .role("ROLE_ADMIN")
                .build();

        List<String> roles = new LinkedList<>(Arrays.asList(
                "ROLE_REPRESENTATIVE",
                "ROLE_RESIDENT",
                "ROLE_USER_MANAGER",
                "ROLE_CHIPS_UPDATER",
                "ROLE_SCHOOL_MANAGER",
                "ROLE_ADMIN"
        ));

        Mockito.when(rolesRepository.save(argumentCaptor.capture())).thenReturn(roleByUser);
        Mockito.when(rolesRepository.findRolesByUser("userId")).thenReturn(roles);

        // When:
        List<String> rolesByUser = userService.addRole("userId", "ROLE_ADMIN");

        // Then:
        RoleByUser parameter = argumentCaptor.getValue();

        Assertions.assertThat(rolesByUser).isNotNull();
        Assertions.assertThat(rolesByUser.size()).isEqualTo(6);
        Assertions.assertThat(rolesByUser.get(0)).isEqualTo("ROLE_REPRESENTATIVE");
        Assertions.assertThat(rolesByUser.get(1)).isEqualTo("ROLE_RESIDENT");
        Assertions.assertThat(rolesByUser.get(2)).isEqualTo("ROLE_USER_MANAGER");
        Assertions.assertThat(rolesByUser.get(3)).isEqualTo("ROLE_CHIPS_UPDATER");
        Assertions.assertThat(rolesByUser.get(4)).isEqualTo("ROLE_SCHOOL_MANAGER");
        Assertions.assertThat(rolesByUser.get(5)).isEqualTo("ROLE_ADMIN");
        Assertions.assertThat(parameter).isNotNull();
        Assertions.assertThat(parameter.getId()).isNotEqualTo(roleByUser.getId());
        Assertions.assertThat(parameter.getRole()).isEqualTo("ROLE_ADMIN");
        Assertions.assertThat(parameter.getUserId()).isEqualTo("userId");
    }

    @Test
    public void givenAnEmptyList_whenGetAllUsers_thenEmptyListIsReturned() {
        // Given:
        RolesRepository rolesRepository = Mockito.mock(RolesRepository.class);
        UserRepository userRepository = Mockito.mock(UserRepository.class);
        RepresentativeRepository representativeRepository = Mockito.mock(RepresentativeRepository.class);
        LoginRepository loginRepository = Mockito.mock(LoginRepository.class);

        UserServiceImpl userService = new UserServiceImpl(
                rolesRepository,
                userRepository,
                representativeRepository,
                loginRepository);

        List<org.uresti.pozarreal.model.User> users = new LinkedList<>();

        Mockito.when(userRepository.findAll()).thenReturn(users);

        // When:
        List<User> userList = userService.getAllUsers();

        // Then:
        Assertions.assertThat(userList.isEmpty()).isTrue();
    }

    @Test
    public void givenAnListWithOneElement_whenGetAllUsers_thenListWithOneElementIsReturned() {
        // Given:
        RolesRepository rolesRepository = Mockito.mock(RolesRepository.class);
        UserRepository userRepository = Mockito.mock(UserRepository.class);
        RepresentativeRepository representativeRepository = Mockito.mock(RepresentativeRepository.class);
        LoginRepository loginRepository = Mockito.mock(LoginRepository.class);

        UserServiceImpl userService = new UserServiceImpl(
                rolesRepository,
                userRepository,
                representativeRepository,
                loginRepository);

        List<String> roles = new LinkedList<>(List.of(
                "ROLE_REPRESENTATIVE"
        ));

        List<org.uresti.pozarreal.model.User> users = new LinkedList<>();

        org.uresti.pozarreal.model.User user = org.uresti.pozarreal.model.User.builder()
                .id("userId")
                .picture("picture")
                .name("name")
                .build();

        users.add(user);

        Representative representative = Representative.builder()
                .userId("representativeId")
                .house("house")
                .phone("123456")
                .street("street")
                .build();

        Mockito.when(userRepository.findAll()).thenReturn(users);
        Mockito.when(rolesRepository.findRolesByUser("userId")).thenReturn(roles);
        Mockito.when(representativeRepository.findById("userId")).thenReturn(java.util.Optional.ofNullable(representative));

        // When:
        List<User> userList = userService.getAllUsers();

        // Then:
        Assertions.assertThat(userList).isNotNull();
        Assertions.assertThat(userList.size()).isEqualTo(1);
        Assertions.assertThat(userList.get(0).getRoles().size()).isEqualTo(1);
        Assertions.assertThat(userList.get(0).getId()).isEqualTo("userId");
        Assertions.assertThat(userList.get(0).getPicture()).isEqualTo("picture");
        Assertions.assertThat(userList.get(0).getName()).isEqualTo("name");
        Assertions.assertThat(userList.get(0).getStreet()).isEqualTo("street");
        Assertions.assertThat(userList.get(0).getRoles().get(0)).isEqualTo("ROLE_REPRESENTATIVE");
    }

    @Test
    public void givenWrongEmail_whenBuildUserForEmail_thenThrowNoSuchElementException() {
        // Given:
        RolesRepository rolesRepository = Mockito.mock(RolesRepository.class);
        UserRepository userRepository = Mockito.mock(UserRepository.class);
        RepresentativeRepository representativeRepository = Mockito.mock(RepresentativeRepository.class);
        LoginRepository loginRepository = Mockito.mock(LoginRepository.class);

        UserServiceImpl userService = new UserServiceImpl(
                rolesRepository,
                userRepository,
                representativeRepository,
                loginRepository);

        // When:
        // Then:
        Assertions.assertThatThrownBy(() -> userService.buildUserForEmail("email.com"))
                .isInstanceOf(NoSuchElementException.class);

        Mockito.verifyNoInteractions(rolesRepository);
    }

    @Test
    public void givenAnEmail_whenBuildUserForEmail_thenUserIsReturned() {
        // Given:
        RolesRepository rolesRepository = Mockito.mock(RolesRepository.class);
        UserRepository userRepository = Mockito.mock(UserRepository.class);
        RepresentativeRepository representativeRepository = Mockito.mock(RepresentativeRepository.class);
        LoginRepository loginRepository = Mockito.mock(LoginRepository.class);

        UserServiceImpl userService = new UserServiceImpl(
                rolesRepository,
                userRepository,
                representativeRepository,
                loginRepository);

        List<String> roles = new LinkedList<>(List.of(
                "ROLE_REPRESENTATIVE"
        ));

        org.uresti.pozarreal.model.User user = org.uresti.pozarreal.model.User.builder()
                .id("userId")
                .name("name")
                .picture("picture")
                .build();

        Mockito.when(userRepository.findByEmail("email@email.com")).thenReturn(java.util.Optional.ofNullable(user));
        Mockito.when(rolesRepository.findRolesByUser("userId")).thenReturn(roles);

        // When:
        User userForEmail = userService.buildUserForEmail("email@email.com");

        // Then:
        Assertions.assertThat(userForEmail).isNotNull();
        Assertions.assertThat(userForEmail.getRoles().size()).isEqualTo(1);
        Assertions.assertThat(userForEmail.getRoles().get(0)).isEqualTo("ROLE_REPRESENTATIVE");
        Assertions.assertThat(userForEmail.getPicture()).isEqualTo("picture");
        Assertions.assertThat(userForEmail.getStreet()).isNull();
        Assertions.assertThat(userForEmail.getStatus()).isNull();
        Assertions.assertThat(userForEmail.getId()).isEqualTo("userId");
        Assertions.assertThat(userForEmail.getName()).isEqualTo("name");
    }

    @Test
    public void givenAnUserWithOutId_whenSave_thenThrowNoSuchElementException() {
        // Given:
        RolesRepository rolesRepository = Mockito.mock(RolesRepository.class);
        UserRepository userRepository = Mockito.mock(UserRepository.class);
        RepresentativeRepository representativeRepository = Mockito.mock(RepresentativeRepository.class);
        LoginRepository loginRepository = Mockito.mock(LoginRepository.class);

        UserServiceImpl userService = new UserServiceImpl(
                rolesRepository,
                userRepository,
                representativeRepository,
                loginRepository);

        User user = User.builder().build();

        // When:
        // Then:
        Assertions.assertThatThrownBy(() -> userService.save(user))
                .isInstanceOf(NoSuchElementException.class);

        Mockito.verifyNoInteractions(rolesRepository);
    }

    @Test
    public void givenAnUser_whenSave_thenUserIsSavedAndReturned() {
        // Given:
        RolesRepository rolesRepository = Mockito.mock(RolesRepository.class);
        UserRepository userRepository = Mockito.mock(UserRepository.class);
        RepresentativeRepository representativeRepository = Mockito.mock(RepresentativeRepository.class);
        LoginRepository loginRepository = Mockito.mock(LoginRepository.class);

        UserServiceImpl userService = new UserServiceImpl(
                rolesRepository,
                userRepository,
                representativeRepository,
                loginRepository);

        List<String> roles = new LinkedList<>(List.of(
                "ROLE_REPRESENTATIVE",
                "ROLE_RESIDENT"
                ));

        org.uresti.pozarreal.model.User user = org.uresti.pozarreal.model.User.builder()
                .id("userId")
                .picture("picture")
                .name("name")
                .build();

        User userDto = User.builder()
                .id("userId")
                .name("name")
                .picture("picture")
                .build();

        Mockito.when(userRepository.findById("userId")).thenReturn(Optional.ofNullable(user));
        Mockito.when(rolesRepository.findRolesByUser("userId")).thenReturn(roles);

        // When:
        User userSaved = userService.save(userDto);

        // Then:
        Assertions.assertThat(userSaved).isNotNull();
        Assertions.assertThat(userSaved.getName()).isEqualTo("name");
        Assertions.assertThat(userSaved.getPicture()).isEqualTo("picture");
        Assertions.assertThat(userSaved.getId()).isEqualTo("userId");
        Assertions.assertThat(userSaved.getRoles().size()).isEqualTo(2);
        Assertions.assertThat(userSaved.getRoles().get(0)).isEqualTo("ROLE_REPRESENTATIVE");
        Assertions.assertThat(userSaved.getRoles().get(1)).isEqualTo("ROLE_RESIDENT");

        assert user != null;
        Mockito.verify(userRepository).save(user);
    }

    @Test
    public void givenAnEmail_whenFindOrRegister_thenReturnUser() {
        // Given:
        RolesRepository rolesRepository = Mockito.mock(RolesRepository.class);
        UserRepository userRepository = Mockito.mock(UserRepository.class);
        RepresentativeRepository representativeRepository = Mockito.mock(RepresentativeRepository.class);
        LoginRepository loginRepository = Mockito.mock(LoginRepository.class);

        UserServiceImpl userService = new UserServiceImpl(
                rolesRepository,
                userRepository,
                representativeRepository,
                loginRepository);

        org.uresti.pozarreal.model.User user = org.uresti.pozarreal.model.User.builder()
                .id("userId")
                .name("name")
                .picture("picture")
                .build();

        Mockito.when(userRepository.findByEmail("email@email.com")).thenReturn(Optional.ofNullable(user));

        // When:
        org.uresti.pozarreal.model.User userServiceOrRegister = userService.findOrRegister("email@email.com", null, null);

        // Then:
        Assertions.assertThat(userServiceOrRegister).isNotNull();
        Assertions.assertThat(userServiceOrRegister.getId()).isEqualTo("userId");
        Assertions.assertThat(userServiceOrRegister.getName()).isEqualTo("name");
        Assertions.assertThat(userServiceOrRegister.getPicture()).isEqualTo("picture");
    }

    @Test
    public void givenAnEmailPictureAndName_whenFindOrRegister_thenFindAndReturnUser() {
        // Given:
        RolesRepository rolesRepository = Mockito.mock(RolesRepository.class);
        UserRepository userRepository = Mockito.mock(UserRepository.class);
        RepresentativeRepository representativeRepository = Mockito.mock(RepresentativeRepository.class);
        LoginRepository loginRepository = Mockito.mock(LoginRepository.class);

        UserServiceImpl userService = new UserServiceImpl(
                rolesRepository,
                userRepository,
                representativeRepository,
                loginRepository);

        ArgumentCaptor<org.uresti.pozarreal.model.User> argumentCaptor = ArgumentCaptor.forClass(org.uresti.pozarreal.model.User.class);

        ArgumentCaptor<Login> argumentCaptorLogin = ArgumentCaptor.forClass(Login.class);

        org.uresti.pozarreal.model.User user = org.uresti.pozarreal.model.User.builder()
                .picture("picture")
                .name("name")
                .id("id")
                .build();

        Login login = Login.builder()
                .id("id")
                .email("email@email.com")
                .userId("userId")
                .build();

        Mockito.when(userRepository.findByEmail(null)).thenReturn(null);
        Mockito.when(userRepository.save(argumentCaptor.capture())).thenReturn(user);
        Mockito.when(loginRepository.save(argumentCaptorLogin.capture())).thenReturn(login);

        // When:
        org.uresti.pozarreal.model.User userServiceOrRegister = userService.findOrRegister("email@email.com", "picture", "name");

        // Then:
        org.uresti.pozarreal.model.User parameter = argumentCaptor.getValue();
        org.uresti.pozarreal.model.Login parameterLogin = argumentCaptorLogin.getValue();

        Assertions.assertThat(parameter).isNotNull();
        Assertions.assertThat(parameter.getId()).isNotEqualTo("id");

        Assertions.assertThat(parameterLogin).isNotNull();
        Assertions.assertThat(parameterLogin.getId()).isNotEqualTo("id");
        Assertions.assertThat(parameterLogin.getEmail()).isEqualTo("email@email.com");
        Assertions.assertThat(parameterLogin.getUserId()).isEqualTo(parameter.getId());

        Assertions.assertThat(userServiceOrRegister).isNotNull();
        Assertions.assertThat(userServiceOrRegister.getPicture()).isEqualTo("picture");
        Assertions.assertThat(userServiceOrRegister.getName()).isEqualTo("name");
        Assertions.assertThat(userServiceOrRegister.getId()).isNotNull();
    }
}