package sptech.school.projetovolt.service.hashtable;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import sptech.school.projetovolt.entity.produto.repository.ProdutoRepository;
import sptech.school.projetovolt.utils.HashTableObj;

@Service
@RequiredArgsConstructor
public class HashTableService {
    private final HashTableObj<String> hashTable;
    private final ProdutoRepository produtoRepository;

    public void inserirProdutos(){
        if(!hashTable.isEmpty()){
            throw new IllegalStateException("Hash table já está preenchida");
        }
        produtoRepository.findAll().stream().forEach(produto -> {
            hashTable.put(produto.getNome().toLowerCase());
        });
    }

    public String buscarProdutoPorNome(String nomeProduto){
        if(hashTable.isEmpty()){
            throw new IllegalStateException("Tabela Hash vazia!");
        }
        return hashTable.get(nomeProduto.toLowerCase());
    }
    public Boolean removerProdutoPorNome(String nomeProduto){
        if(hashTable.isEmpty()){
            throw new IllegalStateException("Tabela Hash vazia!");
        }
        return hashTable.remove(nomeProduto);
    }
    public void exibirProdutos(){
        if(hashTable.isEmpty()){
            throw new IllegalStateException("Tabela Hash vazia!");
        }
        hashTable.show();
    }

}