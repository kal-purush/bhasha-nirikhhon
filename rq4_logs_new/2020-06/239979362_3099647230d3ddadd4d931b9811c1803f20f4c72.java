package com.pesados.purplepoint.api.controller;

import com.pesados.purplepoint.api.exception.NewQuestionBadRequest;
import com.pesados.purplepoint.api.exception.QuestionNotFoundException;
import com.pesados.purplepoint.api.model.question.Question;
import com.pesados.purplepoint.api.model.question.QuestionService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import java.util.List;

@RestController
@RequestMapping("/api/v1")
public class FAQController {

	private final QuestionService faqService;

	public FAQController(QuestionService faqService) {
		this.faqService = faqService;
	}

	@Operation(summary = "Get All FAQs by Language", description = "Get FAQs by the given language.", tags = {"question"})
	@ApiResponses(value = {
			@ApiResponse(responseCode = "200", description = "successful operation",
					content = @Content(array = @ArraySchema(schema = @Schema(implementation = Question.class)))) })
	@GetMapping(value = "/wiki/faqs/{lang}", produces = { "application/json", "application/xml"})
	List<Question> all(
			@Parameter(description = "Language of the desired definitions. Must be esp or en", required = true)
			@RequestParam String lang) {

		return faqService.getQuestionByLanguage(lang);
	}


	@Operation(summary = "Post a new Question", description = "Posts a new question to the FAQ. La primary key de la pregunta es su id.", tags = {"question"})
	@ApiResponses(value = {
			@ApiResponse(responseCode = "200", description = "successful operation",
					content = @Content(array = @ArraySchema(schema = @Schema(implementation = Question.class)))) })
	@PostMapping(value = "/wiki/faqs/create", produces = { "application/json", "application/xml"})
	Question newQuestion(
				@Parameter(description = "Language of the desired definitions. Must be esp or en", required = true)
			@Valid @RequestBody Question newQue) throws NewQuestionBadRequest {
		// 		if (!userService.getUserByEmail(userNew.getEmail()).isPresent()) {
		if (!faqService.getQuestionByID(newQue.getQuestionId()).isPresent()) {
			return this.faqService.saveQuestion(newQue);
		} else {
			throw new NewQuestionBadRequest();
		}
	}

	@Operation(summary = "Delete a Question", description = "Deletes an existing question in the FAQ. La primary key de la pregunta es su id.", tags = {"question"})
	@ApiResponses(value = {
			@ApiResponse(responseCode = "200", description = "successful operation",
					content = @Content(array = @ArraySchema(schema = @Schema(implementation = Question.class)))) })
	@DeleteMapping(value = "/wiki/faqs/delete/{id}", produces = { "application/json", "application/xml"})
	void deleteQuestion(
			@Parameter(description = "Id of the question you want to erase.", required = true)
			@Valid @PathVariable Long id) {
		if (faqService.getQuestionByID(id).isPresent()) {
			this.faqService.deleteById(id);
		} else {
			throw new QuestionNotFoundException(id);
		}
	}
}