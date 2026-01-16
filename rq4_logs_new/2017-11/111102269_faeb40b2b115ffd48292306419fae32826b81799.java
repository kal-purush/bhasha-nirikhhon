package doe.controller;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.util.UriComponentsBuilder;

import doe.model.Doacao;
import doe.model.Usuario;
import doe.repository.DoacaoRepository;
import doe.repository.UsuarioRepository;


@Controller    
@RequestMapping(path="doacao") 
public class DoacaoController {
	
	@Autowired
	private DoacaoRepository doacaoRepository;
	@Autowired
	private UsuarioRepository usuarioRepository;
	
    public static final Logger logger = LoggerFactory.getLogger(DoacaoController.class);
    
 
    @RequestMapping(value = "", method = RequestMethod.GET)
    public ResponseEntity<List<Doacao>> buscarTodos() {
    	List<Doacao> doacoes = doacaoRepository.findAll();
        if (doacoes.isEmpty()) {
            return new ResponseEntity(HttpStatus.NO_CONTENT);
        }
        return new ResponseEntity<List<Doacao>>(doacoes, HttpStatus.OK);
    }
 
    @RequestMapping(value = "{id}", method = RequestMethod.GET)
    public ResponseEntity<?> buscarPorId(@PathVariable("id") long id) {
        Doacao doacao= doacaoRepository.findOne(id);
        if (doacao == null) {
            return new ResponseEntity(new CustomErrorType("Não encontrado!"), HttpStatus.NOT_FOUND);
        }
        return new ResponseEntity<Doacao>(doacao, HttpStatus.OK);
    }
        
    @RequestMapping(value = "", method = RequestMethod.POST)
    public ResponseEntity<?> cadastrarPedido(@RequestBody Doacao doacao, UriComponentsBuilder ucBuilder) {
        if (usuarioRepository.getOne(doacao.getUsuario().getId()) == null) {
            return new ResponseEntity(new CustomErrorType("Não encontrado."), HttpStatus.CONFLICT);
        }
        
        Usuario u = usuarioRepository.getOne(doacao.getUsuario().getId());
        doacao.setUsuario(u);
        doacao = doacaoRepository.save(doacao);
 
        HttpHeaders headers = new HttpHeaders();
        headers.setLocation(ucBuilder.path("/doacao/{id}").buildAndExpand(doacao.getIdDoacao()).toUri());
        
        return new ResponseEntity<String>(headers, HttpStatus.CREATED);
    }
 
    @RequestMapping(value = "{id}", method = RequestMethod.PUT)
    public ResponseEntity<?> atualizar(@PathVariable("id") long id, @RequestBody Doacao doacao) {
        Doacao currentDoacao = doacaoRepository.findOne(id);
 
        if (currentDoacao == null) {
            return new ResponseEntity(new CustomErrorType("Não encontrado!"), HttpStatus.NOT_FOUND);
        }
 
        currentDoacao.setDataCriacao(doacao.getDataCriacao());
        currentDoacao.setUsuario(doacao.getUsuario());
       
        doacaoRepository.save(currentDoacao);
        return new ResponseEntity<Doacao>(currentDoacao, HttpStatus.OK);
    }

    @RequestMapping(value = "{id}", method = RequestMethod.DELETE)
    public ResponseEntity<?> deletar(@PathVariable("id") long id) {
        Doacao doacao = doacaoRepository.findOne(id);
        if (doacao == null) {
            return new ResponseEntity(new CustomErrorType("Não encontrado!"), HttpStatus.NOT_FOUND);
        }
        doacaoRepository.delete(id);
        return new ResponseEntity<Doacao>(HttpStatus.NO_CONTENT);
    } 
}