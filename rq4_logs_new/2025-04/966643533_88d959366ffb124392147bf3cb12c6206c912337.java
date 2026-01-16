package org.spiceboys.Travel.Diary.seeder;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.spiceboys.Travel.Diary.exception.ContentNotFoundException;
import org.spiceboys.Travel.Diary.model.Activity;
import org.spiceboys.Travel.Diary.model.User;
import org.spiceboys.Travel.Diary.model.Country;
import org.spiceboys.Travel.Diary.model.Itinerary;
import org.spiceboys.Travel.Diary.repository.CountryRepository;
import org.spiceboys.Travel.Diary.repository.ItineraryRepository;
import org.spiceboys.Travel.Diary.repository.UserRepository;
import org.spiceboys.Travel.Diary.repository.ActivityRepository;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import java.util.List;
import java.io.InputStream;
import java.util.Optional;

@Component
@Order(4)
class ActivitySeeder implements CommandLineRunner {
    private final ItineraryRepository itineraryRepository;

    private final ActivityRepository activityRepository;

    private final ObjectMapper objectMapper;

    public ActivitySeeder(ItineraryRepository itineraryRepository, ActivityRepository activityRepository,ObjectMapper objectMapper) {
        this.itineraryRepository = itineraryRepository;
        this.activityRepository = activityRepository;
        this.objectMapper = objectMapper;
    }

    @Override
    public void run(String... args) throws Exception {
        if (activityRepository.count() == 0) {
            InputStream resourceAsStream = this.getClass().getResourceAsStream("/data/activities.json");
            if (resourceAsStream != null) {
                List<Activity> activities = objectMapper.readValue(resourceAsStream, new TypeReference<List<Activity>>(){});
                for (Activity activity : activities) {
                    Long itineraryId = activity.getItinerary().getItineraryId();
                    Optional<Itinerary> itinerary = itineraryRepository.findById(itineraryId);
                    if (itinerary.isPresent()) {
                        activity.setItinerary(itinerary.get());
                    } else {
                        throw new ContentNotFoundException("Itinerary not found");
                    }
                }
                activityRepository.saveAll(activities);
                System.out.println("✅ Database seeded with " + activities.size() + " activities");
            } else {
                System.out.println("ℹ Could not find activities.json");
            }
        } else {
            System.out.println("ℹ Database already seeded with activities");
        }
    }
}