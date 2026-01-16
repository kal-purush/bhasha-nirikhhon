package dwbe.lojatenis.DTO;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class FornecedorDTO {
    private String nome;
    private String cpf;
    private String endereco;
    private String sexo;
    private String telefone;
    private String email;
    private String cnpj;
    private int numeroInscricao;
    private String nomeFantasia;
    private String dataDeAbertura;
    private String porte;
    private String atividadeEconomicaPrincipal;
    private String situacaoCadastral;
}