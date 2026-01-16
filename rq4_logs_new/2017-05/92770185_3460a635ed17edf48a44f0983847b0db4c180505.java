package br.com.whatsappandroid.cursoandroid.myeasyparking;

import android.database.Cursor;

import java.util.List;

/**
 * Created by root on 29/05/17.
 */

public interface DAO<T> {

    public boolean salvar(T t);
    public List<T> listar(Cursor cursor);
    public boolean deletar(int id);
    public boolean atualizar(T t);
}