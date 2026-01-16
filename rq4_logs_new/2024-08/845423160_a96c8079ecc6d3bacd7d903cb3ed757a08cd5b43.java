package fiveguys.innout.service;

import fiveguys.innout.dto.AverageSpendingDTO;
import fiveguys.innout.entity.Transaction;
import fiveguys.innout.entity.User;
import fiveguys.innout.repository.TransactionRepository;
import fiveguys.innout.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class AverageSpendingService {

    @Autowired
    private TransactionRepository transactionRepository;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private AgeGroupService ageGroupService;

    public AverageSpendingDTO calculateAverageSpending(Long userId) {
        User user = userRepository.findById(userId)
                .orElseThrow(() -> new RuntimeException("User not found"));

        String userAgeGroup = ageGroupService.calculateAgeGroup(userId);

        // 해당 연령대의 모든 사용자를 가져옴
        List<User> usersInSameAgeGroup = userRepository.findAll().stream()
                .filter(u -> ageGroupService.calculateAgeGroup(u.getId()).equals(userAgeGroup))
                .collect(Collectors.toList());

        // 연령대에 속한 사용자의 총 지출을 계산
        int totalSpending = usersInSameAgeGroup.stream()
                .mapToInt(u -> transactionRepository.findAll().stream()
                        .filter(t -> t.getUser().getId().equals(u.getId()))
                        .mapToInt(Transaction::getAmount)
                        .sum()
                )
                .sum();

        // 연령대 평균 지출을 계산
        int averageSpending = totalSpending / usersInSameAgeGroup.size();

        AverageSpendingDTO dto = new AverageSpendingDTO();
        dto.setUserAgeGroup(userAgeGroup);
        dto.setAverageSpending(averageSpending);

        return dto;
    }
}