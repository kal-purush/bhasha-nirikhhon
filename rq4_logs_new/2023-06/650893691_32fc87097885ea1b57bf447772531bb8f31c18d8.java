package br.com.pablo.chamados.dto;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public class ChamadoPorRegiaoDto {

    String regiao;
    Integer quantidadeDeChamados;
    List<QtdPorLocalidadeDto> localidadesDto;


    public ChamadoPorRegiaoDto() {
        localidadesDto = new ArrayList<>();
    }

    public ChamadoPorRegiaoDto(List<QtdPorLocalidadeDto> lista, String regiao) {
        this.regiao = regiao;
        this.localidadesDto = lista;

        int qtd = 0;
        for (QtdPorLocalidadeDto objs: lista) {
            qtd += objs.getChamados().size();
        }
        this.quantidadeDeChamados =qtd;
    }


}