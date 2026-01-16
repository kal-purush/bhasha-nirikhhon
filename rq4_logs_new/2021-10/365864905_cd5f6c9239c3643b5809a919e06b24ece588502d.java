package com.project.lebiton.dao;

import com.project.lebiton.model.UsuarioInterface;

import java.sql.SQLException;

public interface GeneralUsuarioDaoInterface {
    boolean login(final String usuario, final String senha);

    boolean createUser(final UsuarioInterface usuario) throws SQLException;
}