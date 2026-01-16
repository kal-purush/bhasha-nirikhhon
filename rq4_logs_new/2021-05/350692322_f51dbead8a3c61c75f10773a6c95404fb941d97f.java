package upeldev.com.github.upel3.services;

import org.springframework.stereotype.Service;
import upeldev.com.github.upel3.model.Course;
import upeldev.com.github.upel3.model.achievement.Achievement;
import upeldev.com.github.upel3.repositories.AchievementRepository;

import java.util.List;

@Service
public class AchievementService {

    private final AchievementRepository achievementRepository;

    public AchievementService(AchievementRepository achievementRepository){
        this.achievementRepository = achievementRepository;
    }

    public void save(Achievement achievementDTO){
        achievementRepository.save(achievementDTO);
    }

    public List<Achievement> findAllByCourse(Course course){
        return achievementRepository.findAllByCourse(course);
    }


}