package com.pesados.purplepoint.api.controller;

import com.pesados.purplepoint.api.exception.DefinitionBadRequestException;
import com.pesados.purplepoint.api.exception.UnauthorizedDeviceException;
import com.pesados.purplepoint.api.model.definition.Definition;
import com.pesados.purplepoint.api.model.definition.DefinitionService;
import com.pesados.purplepoint.api.model.report.Report;
import com.pesados.purplepoint.api.model.user.User;
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
public class DefinitionController {

	private final DefinitionService defService;

	public DefinitionController(DefinitionService defService) {
		this.defService = defService;
	}


	@Operation(summary = "Add a new definition",
			description = "Adds a new definition to the database."
			, tags = { "definition" })
	@ApiResponses(value = {
			@ApiResponse(responseCode = "201", description = "Definition created",
					content = @Content(schema = @Schema(implementation = Definition.class))),
			@ApiResponse(responseCode = "400", description = "Invalid input"),
			@ApiResponse(responseCode = "409", description = "Definition already exists") })
	@PostMapping(value = "/definitions", consumes = { "application/json", "application/xml" })
	Definition newDefintion(
			@Parameter(description="Report to add. Cannot null or empty.",
					required=true, schema=@Schema(implementation = Definition.class))
			@Valid @RequestBody Definition newDef
	) {
		if(!defService.getDefinitionByWord(newDef.getWord()).isPresent()){
			return this.defService.saveDefinition(newDef);
		}else {
			throw new DefinitionBadRequestException();
		}
	}


	@Operation(summary = "Get All Definition by Language", description = "Get definitions by the given language ", tags = {"definition"})
	@ApiResponses(value = {
			@ApiResponse(responseCode = "200", description = "successful operation",
					content = @Content(array = @ArraySchema(schema = @Schema(implementation = Report.class)))) })
	@GetMapping(value = "/definitions/{lang}", produces = { "application/json", "application/xml"})
	List<Definition> all(
			@Parameter(description = "Language of the desired definitions. Must be ESP or EN", required = true)
			@PathVariable String lang) {

		return defService.getDefinitionByLanguage(lang);

	}
}