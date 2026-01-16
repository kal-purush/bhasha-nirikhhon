package dao;

import java.util.List;

import domain.Album;

public interface AlbumDao {

	List<Album> getAlbum(Album album);

	void excluir(Album album);

	Album salvar(Album album);

	void atualizar(Album album);

}