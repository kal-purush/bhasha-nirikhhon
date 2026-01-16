package le.ac.uk.controller;

import le.ac.uk.entity.activity.activity;
import le.ac.uk.service.IActivityService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/activity")
public class ActivityController {

    @Autowired
    private IActivityService activityService;

    @PostMapping
    protected List<activity> getBulkActivity(@RequestParam("region") String region, @RequestParam("activityType") int activityType) {
        return activityService.selectActivityByRegionAndType(region, activityType);
    }
}