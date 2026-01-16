package org.wecancodeit.backend.Controllers;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.wecancodeit.backend.BOService.AnimalQuestionService;
import org.wecancodeit.backend.DataModels.AnimalQuestionModel;
import org.wecancodeit.backend.Enums.DifficultyLevel;

import jakarta.annotation.Resource;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

@RestController
@RequestMapping({"api/v1/animals"}) // Define the base URL for all endpoints in this controller
public class AnimalQuestionController {

    @Resource // Injecting the AnimalQuestionService dependency
    private AnimalQuestionService animalQuestionService;

    /**
     * Constructor for AnimalQuestionController that accepts an AnimalQuestionService instance.
     * This enables dependency injection, allowing the service to be used within this controller.
     * 
     * @param animalQuestionService The AnimalQuestionService to be injected.
     */
    public AnimalQuestionController (AnimalQuestionService animalQuestionService) {
        this.animalQuestionService = animalQuestionService;
    }

    /**
     * Endpoint to retrieve a random animal question based on difficulty level.
     *
     * @param difficulty The specified difficulty level to filter questions by.
     * @return A ResponseEntity containing the animal question if found, or a NOT_FOUND status if not.
     */
    @GetMapping("/animals/{difficulty}") // Mapping for GET requests to "/api/v1/animals/{difficulty}"
    public ResponseEntity<AnimalQuestionModel> getRandomAnimalQuestion(@PathVariable DifficultyLevel difficulty) {

        // Retrieve a random question by difficulty level from the service
        AnimalQuestionModel animalQuestion = animalQuestionService.getRandomAnimalQuestion(difficulty);

        // If no question was found for the given difficulty, return a 404 status
        if (animalQuestion == null) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
        // Otherwise, return the question with an OK status
        else {
            return new ResponseEntity<>(animalQuestion, HttpStatus.OK);
        }
    }
}