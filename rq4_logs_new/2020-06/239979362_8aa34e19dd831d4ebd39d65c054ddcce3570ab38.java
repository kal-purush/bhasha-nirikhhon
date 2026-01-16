package com.pesados.purplepoint.api.controller;

import com.pesados.purplepoint.api.model.faq.Faq;
import com.pesados.purplepoint.api.model.faq.FaqService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/v1")
public class FaqController {

	private final FaqService faqService;


	public FaqController(FaqService faqService) {
		this.faqService = faqService;
	}

	@Operation(summary = "Get All FAQs by Language", description = "Get FAQs by the given language ", tags = {"definition"})
	@ApiResponses(value = {
			@ApiResponse(responseCode = "200", description = "successful operation",
					content = @Content(array = @ArraySchema(schema = @Schema(implementation = Faq.class)))) })
	@GetMapping(value = "/faqs", produces = { "application/json", "application/xml"})
	List<Faq> all(
			@Parameter(description = "Language of the desired definitions. Must be esp or en", required = true)
			@RequestParam String lang) {

		return faqService.getFaqByLanguage(lang);

	}
}