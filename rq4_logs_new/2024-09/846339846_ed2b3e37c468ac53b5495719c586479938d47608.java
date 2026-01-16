package reveste.brecho.entity.dto;

import lombok.Getter;
import lombok.Setter;
import reveste.brecho.entity.produto.TamanhoProdutoEnum;
import reveste.brecho.entity.produto.TipoProdutoEnum;

@Getter
@Setter
public class ProdutoDto {

    // Atributos de Produto
    private String nome;
    private TamanhoProdutoEnum tamanho;
    private String cor;
    private String categoria;
    private String subCategoria;
    private Double preco;
    private String descricao;
    private String urlImagem;
    private TipoProdutoEnum tipo;

    // Atributos de ProdutoEspecial
    private String antigoDono;
    private String historia;

}