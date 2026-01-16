package upeldev.com.github.upel3.model.achievement;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import upeldev.com.github.upel3.model.Activity;
import upeldev.com.github.upel3.model.Grade;
import upeldev.com.github.upel3.services.StudentAchievementService;

@Component
public class AchievementManager {

    private final StudentAchievementService studentAchievementService;

    @Autowired
    public AchievementManager(StudentAchievementService studentAchievementService){
        this.studentAchievementService = studentAchievementService;
    }

    @EventListener
    public void handleAchievementEvent(AchievementEvent<Grade> gradeEvent){

        Grade grade = gradeEvent.getValue();
        Activity activity = grade.getActivity();

        if(grade.getValue() == activity.getValue() && activity.getValue() != 0){

            StudentAchievement<Grade> achievement =
                    new GradeAchievement(
                            grade,
                            grade.getUser(),
                            activity.getCourse(),
                            AchievementType.MAXED_ACTIVITIES
                    );

        }
        if(grade.getValue() >= activity.getPassValue()){
            StudentAchievement<Grade> achievement =
                    new GradeAchievement(
                            grade,
                            grade.getUser(),
                            activity.getCourse(),
                            AchievementType.PASSED_ACTIVITIES
                    );
        }

    }
}