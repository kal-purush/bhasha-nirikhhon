package br.ufrn.instancies.DASH.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

import br.ufrn.FormsFramework.controller.FormularioController;
import br.ufrn.FormsFramework.mapper.formulario.FormularioCompleteOutput;
import br.ufrn.FormsFramework.model.Formulario;
@Controller
public class ProntuarioController extends FormularioController {

    @Override
    @GetMapping("/{idFormulario}/informacoesArquivo")
    public ResponseEntity<FormularioCompleteOutput> gerarArquivo(@PathVariable Long idFormulario) {
        Formulario formulario = formularioService.getById(idFormulario, true);
        FormularioCompleteOutput formularioOutput = formularioMapper.toFormularioCompleteOutput(formulario);
        return new ResponseEntity<FormularioCompleteOutput>(formularioOutput, HttpStatus.OK);
    }
}