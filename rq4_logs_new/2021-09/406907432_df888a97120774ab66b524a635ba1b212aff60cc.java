package br.com.zup.TaDentro.colaborador;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;

@RestController
@RequestMapping ("/colaborador")
public class ColaboradorController {

    @Autowired
    private ColaboradorService colaboradorService;

    @PostMapping
    @ResponseStatus (HttpStatus.CREATED)
    public Colaborador salvarColaborador (@RequestBody @Valid Colaborador colaborador) {

        // Converter Model em DTO

        return colaboradorService.salvarColaborador(colaborador);

    }
}