package ufla.zetta.gestao.pecuaria.services.cadastro_positivo_do_pecuarista;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import ufla.zetta.gestao.pecuaria.entity.cadastro_positivo_do_pecuarista.CadastroPositivo;

import java.util.List;

public interface ServiceCadastroPositivo {
    ResponseEntity<CadastroPositivo> insereCadastroPositivo ();
    ResponseEntity<List<CadastroPositivo>> listarCadastrosPositivos ();
    ResponseEntity<CadastroPositivo> buscaCadastroPositivo(String cnpj);
    ResponseEntity<String> editarCadastroPositivo(String cnpj);
    ResponseEntity<String> deletaCadastroPositivo(String cnpj);
}