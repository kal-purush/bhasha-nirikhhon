package reveste.brecho.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.validation.constraints.NotBlank;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Getter @Setter
public class Peca {

    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;
//    private String nome;
    private Integer tamanho;
    private String cor;
    private String marca;
    private String categoria;
    private Double preco;
    private String descricao;
    private String urlImagem;

}