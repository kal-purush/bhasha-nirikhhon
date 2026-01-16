package org.detective.services;

import lombok.RequiredArgsConstructor;
import org.detective.dto.UserPointDTO;
import org.detective.entity.Client;
import org.detective.entity.User;
import org.detective.entity.UserPoint;
import org.detective.repository.ClientRepository;
import org.detective.repository.UserPointRepository;
import org.detective.repository.UserRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;


@RequiredArgsConstructor
@Service
public class UserPointService {

    private final UserPointRepository userPointRepository;
    private final UserRepository userRepository;
    private final ClientRepository clientRepository;

    @Transactional
    public void usePoints(UserPointDTO userPoint) {
        User user = userRepository.findById(userPoint.getUserId())
                .orElseThrow(() -> new IllegalArgumentException("User Not Found"));
        Long userId = user.getUserId();
        Optional<Client> client = clientRepository.findById(userId);


    }
}