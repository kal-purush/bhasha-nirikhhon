package br.com.muambyte.jdbc.dao.usuario;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import br.com.muambyte.jdbc.connection.ConnectionFactory;
import br.com.muambyte.modelo.Usuario;

public class UsuarioDao {

	private Connection connection;

	public UsuarioDao() {
		this.connection = new ConnectionFactory().getConnection();
	}

	public void adiciona(Usuario login) {
		String sql = "insert into usuario " + "(usuario, senha, d_e_l_e_t_e)" + " values (?,?,?)";

		try {
			PreparedStatement stmt = connection.prepareStatement(sql);

			stmt.setString(1, login.getUsuario());
			stmt.setString(2, login.getSenha());
			stmt.setString(3, login.getD_e_l_e_t_e());

			stmt.execute();
			stmt.close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}

	}

	public void altera(Usuario usuario) {
		String sql = "update usuario set usuario=?, senha=?, d_e_l_e_t_e=? " 
					+ "where idUsuario=?";

		try {
			PreparedStatement stmt = connection.prepareStatement(sql);

			stmt.setString(1, usuario.getUsuario());
			stmt.setString(2, usuario.getSenha());
			stmt.setString(3, usuario.getD_e_l_e_t_e());
			stmt.setInt(4, usuario.getIdUsuario());

			stmt.execute();
			stmt.close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}

	}

	public void remove(Usuario usuario) {
		String sql = "update usuario set d_e_l_e_t_e=? " + "where idUsuario=?";

		try {
			PreparedStatement stmt = connection.prepareStatement(sql);

			stmt.setString(1, usuario.getD_e_l_e_t_e());
			stmt.setInt(2, usuario.getIdUsuario());

			stmt.execute();
			stmt.close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public List<Usuario> getLista() {
		try {
			List<Usuario> usuarios = new ArrayList<Usuario>();
			PreparedStatement stmt = this.connection.prepareStatement("select * from usuario where d_e_l_e_t_e=0");
			ResultSet rs = stmt.executeQuery();

			while (rs.next()) {
				Usuario usuario = new Usuario();
				usuario.setIdUsuario(rs.getInt("idUsuario"));
				usuario.setUsuario(rs.getString("usuario"));
				usuario.setSenha(rs.getString("senha"));

				usuarios.add(usuario);
			}
			rs.close();
			stmt.close();
			return usuarios;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

}