package reveste.brecho.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reveste.brecho.entity.Produto;
import reveste.brecho.repository.ProdutoRepository;

import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/produtos")
public class ProdutoController {

    @Autowired
    private ProdutoRepository produtoRepository;

    @GetMapping("/{id}")
    public ResponseEntity<Produto> buscarPorId(@PathVariable int id){

        Optional<Produto> produtoOpt = produtoRepository.findById(id);

        if (produtoOpt.isPresent()){
            return ResponseEntity.status(200).body(produtoOpt.get());
        }
        return ResponseEntity.status(404).build();

    }

    @GetMapping
    public ResponseEntity<List<Produto>> listar() {

        List<Produto> produtos = produtoRepository.findAll();

        if (produtos.isEmpty()){
            return ResponseEntity.status(204).build();
        }
        return ResponseEntity.status(200).body(produtos);

    }

    @PostMapping
    public ResponseEntity<Produto> criar(@RequestBody Produto novoProduto){

        novoProduto.setId(null);
        return ResponseEntity.status(201).body(produtoRepository.save(novoProduto));

    }

    @PutMapping("/{id}")
    public ResponseEntity<Produto> atualizar(@PathVariable int id, @RequestBody Produto produto) {

        if (!produtoRepository.existsById(id)){
            return ResponseEntity.status(404).build();
        }

        produto.setId(id);
        return ResponseEntity.status(200).body(produtoRepository.save(produto));
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deletar(@PathVariable int id) {
        if (produtoRepository.existsById(id)){
            produtoRepository.deleteById(id);

            return ResponseEntity.status(204).build();
        }

        return ResponseEntity.status(404).build();
    }

}