package br.com.zup.TaDentro.Usuario;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import java.util.List;

@RestController
@RequestMapping("/usuarios")
public class UsuarioController {

    @Autowired
    public UsuarioService service;


    @RequestMapping(method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.CREATED)
    public Usuario cadastroUsuario(@RequestBody @Valid Usuario usuario){
        return service.salvarUsuario(usuario);
    }


    @GetMapping
    public List<Usuario> exibirUsuario(){
        return service.exibirUsuario();
    }


}