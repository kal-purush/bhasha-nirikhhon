package le.ac.uk.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import le.ac.uk.model.Route;
import le.ac.uk.repository.ActivityRepository;
import org.springframework.web.bind.annotation.*;

//@RestController
//@RequestMapping("/")
//public class activityController {
//
//    @Autowired
//    private activityDAO activity;
//
//    @GetMapping("/")
//    protected activityDAO getActivity() {
//        return activity;
//    }
//}


import le.ac.uk.model.Weather;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/suggestions")
public class ActivitySuggestionController {

    @Autowired
    private WeatherController weatherController;

    @Autowired
    private ActivityRepository activityRepository;

    @Autowired
    private RouteController routeController;

    @PostMapping()
    public Object getActivity(@RequestParam("city_list") String city) throws JsonProcessingException {

        Weather weather = weatherController.getWeather(city);
        boolean isSuitableForOutdoor = weatherController.checkWeather(weather);

        // Fetch activities based on suitability
        List<Object[]> activities = activityRepository.findByActivityType(city, isSuitableForOutdoor ? 1 : 0);

        // Prepare JSON response
        Map<String, Object> response = new HashMap<>();
        response.put("city", city);
        response.put("weather", weather);
        response.put("isSuitableForOutdoor", isSuitableForOutdoor);

        // Convert activities into a JSON-friendly format
        List<Map<String, Object>> activityList = new ArrayList<>();
        for (Object[] activity : activities) {
            Map<String, Object> activityMap = new HashMap<>();
            activityMap.put("activityName", activity[0]);
            activityMap.put("activityType", activity[1]);
            activityMap.put("activitySolts", activity[2]);
            activityMap.put("activityPrice", activity[3]);
            activityMap.put("ActivityTown", activity[4]);
            activityMap.put("ActivityRegion", activity[5]);
            activityMap.put("ActivityLocationLatitude", activity[6]);
            activityMap.put("ActivityLocationLongitude", activity[7]);
            activityMap.put("ActivityLocationPostcode", activity[8]);

            activityMap.put("id",activity[9]);

            Route route = routeController.getRouteInfo((double)activity[6],(double)activity[7]);  // lat,long
            activityMap.put("ActivityRouteDistance", route.getDistance());
//            System.out.println("Distance is : "+route.getDistance());
            activityMap.put("ActivityRouteTime", route.getEstimatedTime());

            // Add other activity fields as necessary
            activityList.add(activityMap);
        }

        response.put("activities", activityList);

        return response; // Return JSON response

    }
}