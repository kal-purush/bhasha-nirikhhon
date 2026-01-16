package by.it.medved.controllers;

import by.it.medved.dto.DescriptionResponse;
import by.it.medved.services.DescriptionService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("api/v1")
@RequiredArgsConstructor
public class DescriptionController {

    private final DescriptionService descriptionService;

    @GetMapping("/description/{fileName}")
    public DescriptionResponse getDescription(@PathVariable String fileName) {
        return descriptionService.getDescription(fileName);
    }
}