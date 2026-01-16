package api.walking;

import api.common.util.http.HttpResponse;
import api.walking.dto.WalkingDto;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@RequestMapping("/api/walking")
@Controller
public class WalkingController {
    private final WalkingService walkingService;
    public WalkingController(WalkingService walkingService){
        this.walkingService = walkingService;
    }
    @PostMapping("/addWalking")
    @ResponseBody
    public ResponseEntity<Object> addWalking(@RequestBody WalkingDto walkingDto){
        try {
            WalkingDto addedWalking = walkingService.addWalking(walkingDto);
            return HttpResponse.successCreated("Walking successfully added.", addedWalking);
        } catch (Exception e) {
            return HttpResponse.badRequest("Error adding walking: " + e.getMessage(), null);
        }
    }
    @PostMapping("/deleteWalking")
    @ResponseBody
    public ResponseEntity<Object> deleteWalking(@RequestBody WalkingDto walkingDto) {
        try {
            WalkingDto deletedWalking = walkingService.deleteWalking(walkingDto);
            return HttpResponse.successOk("Walking successfully deleted.", deletedWalking);
        } catch (Exception e) {
            return HttpResponse.badRequest("Error deleting walking: " + e.getMessage(), null);
        }
    }
}