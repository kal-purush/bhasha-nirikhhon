package dat3.adventureXP.api;

import dat3.adventureXP.dto.ActivityResponse;
import dat3.adventureXP.service.ActivityService;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@CrossOrigin
@RequestMapping("api/activites")
public class ActivityController {
    ActivityService activityService;

    public ActivityController(ActivityService activityService) {
        this.activityService = activityService;
    }
    @GetMapping
    List<ActivityResponse> getActivities() {
        return activityService.getActivities();
    }

}