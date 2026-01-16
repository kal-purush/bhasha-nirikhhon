package com.chapssal_tteok.preview.domain.user.repository;

import com.chapssal_tteok.preview.domain.user.entity.User;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface UserRepository extends JpaRepository<User, Long> {

    boolean existsByUsername(String username);

    boolean existsByEmail(String email);

    Optional<User> findById(Long id);

    Optional<User> findByUsername (String username);

    Optional<User> findByEmail (String email);
}