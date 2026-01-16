package api.user.owner;

import api.user.enums.Gender;
import api.user.enums.Role;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.ActiveProfiles;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.assertj.core.api.Assertions.assertThat;

@DataJpaTest
@ActiveProfiles("test")
class OwnerRepositoryTest {
    @Autowired
    private OwnerRepository ownerRepository;

    private Owner createOwner() {
        return new Owner("owner@example.com", Role.OWNER, "password", "123456789", Gender.MALE, LocalDate.of(1990, 1, 1));
    }

    @Test
    @DisplayName("owner 저장 테스트")
    void TestSaveAndFindById() {
        Owner savedOwner = ownerRepository.save(createOwner());
        Optional<Owner> foundOwner = ownerRepository.findById(savedOwner.getId());
        assertTrue(foundOwner.isPresent());
        assertThat(foundOwner.get().getEmail()).isEqualTo("owner@example.com");
    }

    @Test
    @DisplayName("모든 유저 조회")
    void TestFindAll() {
        ownerRepository.save(createOwner());
        ownerRepository.save(new Owner("owner2@test.com", Role.OWNER, "testpass2", "0987654321", Gender.FEMALE, LocalDate.of(1995, 5, 15)));
        List<Owner> owners = ownerRepository.findAll();
        assertThat(owners.size()).isGreaterThanOrEqualTo(2);
    }

    @Test
    @DisplayName("id로 owner 삭제 테스트")
    void TestDeleteById() {
        Owner savedOwner = ownerRepository.save(createOwner());
        Integer id = savedOwner.getId();
        ownerRepository.deleteById(id);
        Optional<Owner> foundOwner = ownerRepository.findById(id);
        assertFalse(foundOwner.isPresent());
    }

    @Test
    @DisplayName("owner 수 조회 테스트")
    void TestCount() {
        ownerRepository.save(createOwner());

        long count = ownerRepository.count();
        assertTrue(count >= 1);
    }
}