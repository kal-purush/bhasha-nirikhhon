package org.spiceboys.Travel.Diary.controller;

import org.spiceboys.Travel.Diary.exception.ItineraryNotFoundException;
import org.spiceboys.Travel.Diary.model.Activity;
import org.spiceboys.Travel.Diary.service.ActivityService;
import org.spiceboys.Travel.Diary.service.ItineraryService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class ActivityController {
    private final ActivityService activityService;

    public ActivityController(ActivityService activityService, ItineraryService itineraryService) {
        this.activityService = activityService;
    }

    @PostMapping("/activities")
    public ResponseEntity<Activity> createActivity(
            @RequestBody Activity activity
    ) {
        Activity createdActivity = activityService.createActivity(activity);
        return ResponseEntity.status(HttpStatus.CREATED).body(createdActivity);
    }

    @GetMapping("/activities")
    public ResponseEntity<List<Activity>> getAllActivities() {
        return ResponseEntity.ok(activityService.getAllActivities());
    }

    @GetMapping("/itineraries/{itineraryId}/activities")
    public ResponseEntity<List<Activity>> getActivitiesByItineraryId(@PathVariable Long itineraryId) {
        try {
            List<Activity> activities = activityService.getActivitiesByItineraryId(itineraryId);
            return ResponseEntity.ok(activities);
        } catch (ItineraryNotFoundException e) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(null);
        }
    }

    @PatchMapping("/activities/{activityId}")
    public ResponseEntity<Activity> updateActivity(
            @PathVariable Long activityId,
            @RequestBody Map<String, Object> updatedFields) {
    Activity updatedActivity = activityService.updateActivity(activityId, updatedFields);
    return ResponseEntity.status(HttpStatus.OK).body(updatedActivity);
    }

    @DeleteMapping("/activities/{activityId}")
    public ResponseEntity<String> deleteActivity(@PathVariable Long activityId) {
        boolean isActivityDeleted = activityService.deleteActivityByActivityId(activityId);
        if (isActivityDeleted){
            return ResponseEntity.status(HttpStatus.OK).body("Activity deleted successfully");
        } else {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("Activity not found");
        }
    }
}