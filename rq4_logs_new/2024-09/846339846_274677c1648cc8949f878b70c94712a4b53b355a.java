package reveste.brecho.controller;

import com.gtbr.ViaCepClient;
import com.gtbr.domain.Cep;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reveste.brecho.entity.Endereco;
import reveste.brecho.repository.EnderecoRepository;
import reveste.brecho.service.ViaCepService;
import reveste.brecho.utils.Ordenador;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;

@RestController
@RequestMapping("/enderecos")
public class EnderecoController {

    private final EnderecoRepository repository;

    public EnderecoController(EnderecoRepository repository) {
        this.repository = repository;
    }


    @GetMapping("/ordenar")
    public ResponseEntity<List<Endereco>> ordenadoPorCidade(@RequestParam String estado, String cidade, String logradouro) {
        List<Endereco> enderecos = ViaCepService.buscarPorEndereco(estado, cidade, logradouro);

        Ordenador.ordenarPorLogradouro(enderecos);

        return ResponseEntity.ok(enderecos);
    }
}