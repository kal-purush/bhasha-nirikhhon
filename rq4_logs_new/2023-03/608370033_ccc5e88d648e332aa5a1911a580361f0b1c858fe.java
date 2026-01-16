package pl.javastart.bootcamp.domain.user;

import org.springframework.data.jpa.repository.JpaRepository;
import pl.javastart.bootcamp.domain.user.role.UserRole;

import java.util.Optional;

public interface UserRoleRepository extends JpaRepository<UserRole, Long> {
    Optional<UserRole> findAllByUser(User user);
}