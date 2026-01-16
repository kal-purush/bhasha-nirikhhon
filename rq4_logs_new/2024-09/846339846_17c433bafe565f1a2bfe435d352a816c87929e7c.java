package reveste.brecho;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/usuarios")
public class UsuarioController {

    /*
    Create - Post
    Read - Get
    Update - Put/Patch
    Delete - Delete
     */

    @Autowired
    private UsuarioRepository usuarioRepository;

    // Get do usuário pelo id
    @GetMapping("/{id}")
    public ResponseEntity<Usuario> buscarPorId(@PathVariable int id){
        Optional<Usuario> usuarioOpt = usuarioRepository.findById(id);

        if (usuarioOpt.isPresent()){
            return ResponseEntity.status(200).body(usuarioOpt.get());
        }
        return ResponseEntity.status(404).build();

    }

    // Get de todos os usuários
    @GetMapping
    public ResponseEntity<List<Usuario>> listar() {

        List<Usuario> usuarios = usuarioRepository.findAll();

        if (usuarios.isEmpty()){
            return ResponseEntity.status(204).build();
        }
        return ResponseEntity.status(200).body(usuarios);
    }

    // Post
    @PostMapping
    public ResponseEntity<Usuario> criar(@RequestBody Usuario novoUsuario){

        novoUsuario.setId(null);
        return ResponseEntity.status(201).body(usuarioRepository.save(novoUsuario));

    }

    // Put
    @PutMapping("/{id}")
    public ResponseEntity<Usuario> atualizar(
        @PathVariable int id,
        @PathVariable Usuario usuario
    ) {
        if (!usuarioRepository.existsById(id)){
            return ResponseEntity.status(404).build();
        }
        usuario.setId(id);
        Usuario usuarioAtualizado = usuarioRepository.save(usuario);
        return ResponseEntity.status(200).body(usuarioAtualizado);
    }

    // Delete
    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deletar(@PathVariable int id){

        if (usuarioRepository.existsById(id)){
            usuarioRepository.deleteById(id);
            return ResponseEntity.status(204).build();
        }
        return ResponseEntity.status(404).build();

    }

}