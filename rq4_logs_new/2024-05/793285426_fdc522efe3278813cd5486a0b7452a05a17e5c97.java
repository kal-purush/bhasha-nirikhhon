package com.funcionario.client.endereco.dto;

import com.funcionario.domain.empregado.model.Empregado;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class LocalidadesDto {

    private String cep;
    private String logradouro;
    private String bairro;
    private String localidade;
    private String uf;
    private String complemento;
    private String ibge;
    private String gia;
    private String ddd;
    private String siafi;

   public static void cadastrarLocalidade(Empregado empregado, LocalidadesDto localidadesDto) {
        empregado.getEndereco().setCep(localidadesDto.getCep());
        empregado.getEndereco().setRua(localidadesDto.getLogradouro());
        empregado.getEndereco().setBairro(localidadesDto.getBairro());
        empregado.getEndereco().setCidade(localidadesDto.getLocalidade());
        empregado.getEndereco().setEstado(localidadesDto.getUf());
    }

}