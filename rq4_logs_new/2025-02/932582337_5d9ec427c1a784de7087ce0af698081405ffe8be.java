package com.j30n.stoblyx.adapter.out.persistence;

import com.j30n.stoblyx.domain.user.User;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataIntegrityViolationException;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("UserPersistenceAdapter 테스트")
class UserPersistenceAdapterTest {

    @Mock
    private UserRepository userRepository;

    @Mock
    private UserMapper userMapper;

    @InjectMocks
    private UserPersistenceAdapter userPersistenceAdapter;

    private User testUser;
    private UserJpaEntity testUserJpaEntity;

    @BeforeEach
    void setUp() {
        testUser = User.builder()
                .id(1L)
                .email("test@example.com")
                .password("password123")
                .name("Test User")
                .role(User.Role.USER)
                .build();

        testUserJpaEntity = new UserJpaEntity();
        testUserJpaEntity.setId(1L);
        testUserJpaEntity.setEmail("test@example.com");
        testUserJpaEntity.setPassword("password123");
        testUserJpaEntity.setName("Test User");
        testUserJpaEntity.setRole(UserJpaEntity.Role.USER);
    }

    @Test
    @DisplayName("사용자 저장 - 성공")
    void save_ValidUser_ReturnsSavedUser() {
        // given
        when(userMapper.toJpaEntity(testUser)).thenReturn(testUserJpaEntity);
        when(userRepository.save(testUserJpaEntity)).thenReturn(testUserJpaEntity);
        when(userMapper.toDomainEntity(testUserJpaEntity)).thenReturn(testUser);

        // when
        User savedUser = userPersistenceAdapter.save(testUser);

        // then
        assertThat(savedUser).isNotNull();
        assertThat(savedUser.getEmail()).isEqualTo(testUser.getEmail());
        verify(userRepository).save(any(UserJpaEntity.class));
    }

    @Test
    @DisplayName("사용자 저장 - null 입력 시 예외 발생")
    void save_NullUser_ThrowsException() {
        assertThatThrownBy(() -> userPersistenceAdapter.save(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("User cannot be null");
    }

    @Test
    @DisplayName("사용자 저장 - 이메일 중복 시 예외 발생")
    void save_DuplicateEmail_ThrowsException() {
        // given
        when(userMapper.toJpaEntity(testUser)).thenReturn(testUserJpaEntity);
        when(userRepository.save(any(UserJpaEntity.class)))
                .thenThrow(new DataIntegrityViolationException("Duplicate email"));

        // then
        assertThatThrownBy(() -> userPersistenceAdapter.save(testUser))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    @DisplayName("ID로 사용자 조회 - 성공")
    void findById_ExistingId_ReturnsUser() {
        // given
        given(userRepository.findById(1L)).willReturn(Optional.of(testUserJpaEntity));
        given(userMapper.toDomainEntity(testUserJpaEntity)).willReturn(testUser);

        // when
        Optional<User> foundUser = userPersistenceAdapter.findById(1L);

        // then
        assertThat(foundUser).isPresent();
        assertThat(foundUser.get().getEmail()).isEqualTo(testUser.getEmail());
    }

    @Test
    @DisplayName("ID로 사용자 조회 - 존재하지 않는 ID")
    void findById_NonExistingId_ReturnsEmpty() {
        // given
        given(userRepository.findById(999L)).willReturn(Optional.empty());

        // when
        Optional<User> foundUser = userPersistenceAdapter.findById(999L);

        // then
        assertThat(foundUser).isEmpty();
    }

    @Test
    @DisplayName("이메일로 사용자 조회 - 성공")
    void findByEmail_ExistingEmail_ReturnsUser() {
        // given
        given(userRepository.findByEmail("test@example.com"))
                .willReturn(Optional.of(testUserJpaEntity));
        given(userMapper.toDomainEntity(testUserJpaEntity)).willReturn(testUser);

        // when
        Optional<User> foundUser = userPersistenceAdapter.findByEmail("test@example.com");

        // then
        assertThat(foundUser).isPresent();
        assertThat(foundUser.get().getEmail()).isEqualTo("test@example.com");
    }

    @Test
    @DisplayName("이메일로 사용자 조회 - 존재하지 않는 이메일")
    void findByEmail_NonExistingEmail_ReturnsEmpty() {
        // given
        given(userRepository.findByEmail("nonexistent@example.com"))
                .willReturn(Optional.empty());

        // when
        Optional<User> foundUser = userPersistenceAdapter.findByEmail("nonexistent@example.com");

        // then
        assertThat(foundUser).isEmpty();
    }

    @Test
    @DisplayName("이메일 존재 여부 확인 - 존재하는 이메일")
    void existsByEmail_ExistingEmail_ReturnsTrue() {
        // given
        given(userRepository.existsByEmail("test@example.com")).willReturn(true);

        // when
        boolean exists = userPersistenceAdapter.existsByEmail("test@example.com");

        // then
        assertThat(exists).isTrue();
    }

    @Test
    @DisplayName("이메일 존재 여부 확인 - 존재하지 않는 이메일")
    void existsByEmail_NonExistingEmail_ReturnsFalse() {
        // given
        given(userRepository.existsByEmail("nonexistent@example.com")).willReturn(false);

        // when
        boolean exists = userPersistenceAdapter.existsByEmail("nonexistent@example.com");

        // then
        assertThat(exists).isFalse();
    }
} 