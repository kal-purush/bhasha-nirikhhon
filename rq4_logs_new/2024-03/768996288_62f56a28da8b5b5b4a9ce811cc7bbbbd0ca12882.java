package api.dog;

import api.common.util.http.HttpResponse;
import api.dog.dto.DogDto;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@RequestMapping("/api/dogs")
@Controller
public class DogController {
    private final DogService dogService;

    public DogController(DogService dogService) {
        this.dogService = dogService;
    }

    @PostMapping("/createDog")
    @ResponseBody
    public ResponseEntity<Object> createDog(@RequestBody DogDto dogDto) {
        try {
            DogDto addedDog = dogService.createDog(dogDto);
            return HttpResponse.successCreated("Dog successfully created.", addedDog);
        } catch (Exception e) {
            return HttpResponse.badRequest("Error creating dog: " + e.getMessage(), dogDto);
        }
    }

    @PostMapping("/deleteDog")
    @ResponseBody
    public ResponseEntity<Object> deleteDog(@RequestBody DogDto dogDto) {
        try {
            DogDto deletedDog = dogService.deleteDog(dogDto);
            return HttpResponse.successOk("Dog successfully deleted.", deletedDog);
        } catch (Exception e) {
            return HttpResponse.badRequest("Error deleting dog: " + e.getMessage(), dogDto);
        }
    }
}