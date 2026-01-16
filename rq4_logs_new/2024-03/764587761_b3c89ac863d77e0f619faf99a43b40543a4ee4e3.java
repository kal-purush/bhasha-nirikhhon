package km.cd.backend.challenge.service;

import km.cd.backend.challenge.domain.Challenge;
import km.cd.backend.challenge.dto.ChallengeDto;
import km.cd.backend.challenge.dto.ChallengeResponseDto;
import km.cd.backend.challenge.repository.ChallengeRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class ChallengeService {
    private final ChallengeRepository challengeRepository;

    public ChallengeService(ChallengeRepository challengeRepository) {
        this.challengeRepository = challengeRepository;
    }

    @Transactional
    public ChallengeResponseDto saveChallenge(ChallengeDto challengeDTO) {
        Challenge challenge = challengeDTO.toEntity();
        challengeRepository.save(challenge);
        return ChallengeResponseDto.toDto(challenge);
    }

    @Transactional(readOnly = true)
    public ChallengeResponseDto getChallenge(int id) {
        Challenge challenge = challengeRepository.findById(id).orElseThrow();
        return ChallengeResponseDto.toDto(challenge);
    }
}