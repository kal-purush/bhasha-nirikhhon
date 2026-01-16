package com.lojaonline.hermanos.br.models.dto.carrinho;

import com.lojaonline.hermanos.br.models.Carrinho;
import com.lojaonline.hermanos.br.models.Produto;

import java.util.List;

public record DadosListagemCarrinho(Long idCarrinho, List<Produto> produtos) {
    public DadosListagemCarrinho(Carrinho carrinho) {
        this(
                carrinho.getId(),
                carrinho.getProdutos());
    }
}