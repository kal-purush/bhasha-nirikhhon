package br.com.codenation.v1.errorManager.controller;

import br.com.codenation.v1.errorManager.dto.ApplicationDTO;
import br.com.codenation.v1.errorManager.dto.ApplicationInfoDTO;
import br.com.codenation.v1.errorManager.service.ApplicationService;
import io.swagger.annotations.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import java.util.List;

import static org.springframework.http.HttpStatus.CREATED;
import static org.springframework.http.HttpStatus.NO_CONTENT;


@RestController
@RequestMapping("/api/v1/applications")
@Api("API Application")
public class ApplicationController {

    @Autowired
    ApplicationService applicationService;

    @GetMapping
    @ApiOperation("Obtém uma lista de aplicações.")
    public List<ApplicationInfoDTO> findApplications(ApplicationInfoDTO filtro) {
        return applicationService.findApplications(filtro);
    }

    @GetMapping("/all")
    @ApiOperation("Obtém todos as aplicações do usuário, ativa ou inativa")
    public List<ApplicationInfoDTO> findApplications(){
        return applicationService.findByUserId();
    }


    @PostMapping
    @ResponseStatus(CREATED)
    @ApiOperation("Persiste uma nova aplicação.")
    @ApiResponses({
            @ApiResponse(code = 201, message = "Aplicação salva com sucesso."),
            @ApiResponse(code = 400, message = "Erro de validação.")
    })
    public void insertApplication(@Valid @RequestBody ApplicationDTO name){
        applicationService.saveApplication(name);
    }

    @DeleteMapping("{id}")
    @ResponseStatus(NO_CONTENT)
    @ApiOperation("Desativa uma aplicação.")
    @ApiResponse(code = 204, message = "Aplicação desativada com sucesso.")
    public void deleteApplication(@ApiParam("Id da aplicação") @PathVariable  Long id){
        applicationService.deleteApplication(id);
    }


}