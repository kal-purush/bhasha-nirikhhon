package med.voll.api.medico;

import jakarta.validation.constraints.NotNull;
import med.voll.api.endereco.Endereco;

public record DadosUpdateMedico(
        @NotNull
        Long id,
        String nome,
        String telefone,
        /*No caso do Endereço usaremos o mesmo, como não tem nenhuma especificação, se necessitasse de algo específico ai mudaríamos
        * criando outro DTO de Endereço somente para esse caso. */
        Endereco endereco
        ) {



    public DadosUpdateMedico(Medico dadosMedico){
        this(dadosMedico.getId(), dadosMedico.getNome(), dadosMedico.getTelefone(), dadosMedico.getEndereco());
    }
}