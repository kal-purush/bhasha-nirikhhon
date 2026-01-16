/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.renatoandrade.exportadordeprodutoscsv;

import java.util.Arrays;
import java.util.List;

/**
 *
 * @author joaoh
 */
public abstract class AbstractExportadorListaProduto implements ExportadorListaProduto {

    protected static List<String> TITULOS_COLUNAS = Arrays.asList("ID", "Descrição", "Marca", "Modelo", "Estoque");

    @Override
    public final String exportar(List<Produto> listaProdutos) {
        final StringBuilder sb = new StringBuilder();
        sb.append(abrirTabela());

        sb.append(gerarColunasLinha(TITULOS_COLUNAS));
        sb.append(fecharLinhaTitulos());
        gerarLinhasProdutos(sb, listaProdutos);

        sb.append(fecharTabela());
        return sb.toString();
    }

    private void gerarLinhasProdutos(StringBuilder sb, List<Produto> listaProdutos) {
        for (Produto produto : listaProdutos) {
            List<String> valoresCamposProduto = 
            Arrays.asList(String.valueOf(produto.getId()),produto.getDescricao(),produto.getMarca(),produto.getModelo(),String.valueOf(produto.getEstoque()));
            sb.append(gerarColunasLinha(valoresCamposProduto));
        }
    }

    protected String gerarColunasLinha(List<String> valores) {
        StringBuilder sb = new StringBuilder();
        sb.append(abrirLinha());
        valores.forEach(valor -> {
            if (valor.equals(valores.get(4))) {
                sb.append(abrirColuna(valor));
                return;
            }else{
            sb.append(abrirColuna(valor)).append(fecharColuna());
            }
        });
        sb.append(fecharLinha());
        return sb.toString();
    }

}