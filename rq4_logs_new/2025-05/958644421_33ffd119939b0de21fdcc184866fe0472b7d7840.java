package pl.edu.pk.student.kittysecurity.services;

import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import pl.edu.pk.student.kittysecurity.dto.user.UserResponseDto;
import pl.edu.pk.student.kittysecurity.entity.User;
import pl.edu.pk.student.kittysecurity.exception.custom.UserNotFoundException;
import pl.edu.pk.student.kittysecurity.repository.UserRepository;

import java.util.Optional;

@Service
public class UserService {

    private final UserRepository userRepo;

    public UserService(UserRepository userRepo) {
        this.userRepo = userRepo;
    }


    public ResponseEntity<UserResponseDto> getUserData(Integer userId) {
        Optional<User> foundUser = userRepo.findById(userId);

        if(foundUser.isEmpty()) throw new UserNotFoundException(userId);

        return ResponseEntity.ok().body(UserResponseDto.builder()
                .email(foundUser.get().getEmail())
                .id(foundUser.get().getId())
                .username(foundUser.get().getDisplayName())
                .createdAt(foundUser.get().getCreatedAt().toEpochMilli())
                .updatedAt(foundUser.get().getUpdatedAt().toEpochMilli())
                .build()
        );
    }
}