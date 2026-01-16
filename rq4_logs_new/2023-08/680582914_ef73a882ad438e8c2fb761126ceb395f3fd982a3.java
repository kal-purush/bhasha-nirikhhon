package br.com.sistemadefilmes.repository;

import br.com.sistemadefilmes.model.Filme;

public interface FilmeRepository {

    void buscarFilmePorNome(String nome);

    void save(Filme filme);

}