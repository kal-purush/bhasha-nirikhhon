package com.searchvids.bootstrap;

import com.searchvids.model.Role;
import com.searchvids.model.RoleName;
import com.searchvids.model.User;
import com.searchvids.model.Video;
import com.searchvids.repository.RoleRepository;
import com.searchvids.repository.UserRepository;
import com.searchvids.repository.VideoRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
@Profile({"dev", "default"})
public class BootstrapData implements ApplicationListener<ContextRefreshedEvent> {

    private UserRepository userRepository;
    private RoleRepository roleRepository;
    private PasswordEncoder encoder;

    private final static Logger LOGGER = LoggerFactory.getLogger(BootstrapData.class);

    public BootstrapData(UserRepository userRepository, RoleRepository roleRepository, PasswordEncoder encoder) {
        this.userRepository = userRepository;
        this.roleRepository = roleRepository;
        this.encoder = encoder;
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent contextRefreshedEvent) {

        if (userRepository.count() >= 2) {
            LOGGER.info("Data already loaded");
        } else {
            LOGGER.info("Data is being loaded");
            loadUsers();
        }
    }

    private void loadUsers() {
        // Save roles
        Role role = new Role();
        role.setName(RoleName.ROLE_USER);

        Set<Role> roles = Stream.of(role).collect(Collectors.toSet());

        roleRepository.saveAll(roles);

        // Save User
        User user = new User("username", "test@test.com",
                encoder.encode("password"), roles, new HashSet<>());
        User user2 = new User("username2", "test2@test.com",
                encoder.encode("password"), roles, new HashSet<>());

        List users = Arrays.asList(user, user2);

        userRepository.saveAll(users);

    }
}