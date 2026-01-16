package com.savitoh.casacodigoapi.payload;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.savitoh.casacodigoapi.model.Categoria;
import com.savitoh.casacodigoapi.model.Livro;
import org.springframework.util.Assert;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public class LivroDetalheResponse {

    private final String titulo;

    private final String resumo;

    private final String sumario;

    private final BigDecimal preco;

    private final Integer numeroPaginas;

    private final String isbn;

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "dd/MM/yyyy HH:mm:ss")
    private LocalDateTime dataLancamento;

    private CategoriaDetalheResponse categoriaDetalheResponse;

    public LivroDetalheResponse(String titulo, String resumo, String sumario, BigDecimal preco, Integer numeroPaginas, String isbn, LocalDateTime dataLancamento, Categoria categoria) {
        this.titulo = titulo;
        this.resumo = resumo;
        this.sumario = sumario;
        this.preco = preco;
        this.numeroPaginas = numeroPaginas;
        this.isbn = isbn;
        this.dataLancamento = dataLancamento;
        this.categoriaDetalheResponse = CategoriaDetalheResponse.transformaEntityParaDto(categoria);
    }

    public String getTitulo() {
        return titulo;
    }

    public String getResumo() {
        return resumo;
    }

    public String getSumario() {
        return sumario;
    }

    public BigDecimal getPreco() {
        return preco;
    }

    public Integer getNumeroPaginas() {
        return numeroPaginas;
    }

    public String getIsbn() {
        return isbn;
    }

    public LocalDateTime getDataLancamento() {
        return dataLancamento;
    }

    public CategoriaDetalheResponse getCategoriaDetalheResponse() {
        return categoriaDetalheResponse;
    }

    public static LivroDetalheResponse transformaEntityParaDto(Livro livro) {
        Assert.notNull(livro, "Livro não pode ser nulo");
        return new LivroDetalheResponse(livro.getTitulo(), livro.getResumo(), livro.getSumario(), livro.getPreco(), livro.getNumeroPaginas(), livro.getIsbn(), livro.getDataLancamento(), livro.getCategoria());
    }
}