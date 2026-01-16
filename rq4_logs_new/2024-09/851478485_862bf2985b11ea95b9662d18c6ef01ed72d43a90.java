package org.detective.services.client;

import lombok.RequiredArgsConstructor;
import org.detective.entity.Client;
import org.detective.repository.ClientRepository;
import org.detective.util.CustomUserDetails;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class ClientService {

    private final ClientRepository clientRepository;

    @Transactional
    public Long currentPoints(Long userId) {
        Client client = clientRepository.findByUser_userId(userId);
        return client.getCurrentPoints();
    }
}