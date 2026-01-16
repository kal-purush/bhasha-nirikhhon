package DAO;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import models.Livro;
import persistence.BancoDados;

public class LivroDAO {

    private static final String SELECT_ALL_LIVROS = "SELECT * FROM livros";
    private static final String SELECT_LIVRO_BY_GENERO = "SELECT * FROM livros WHERE genero = ?";
    private static final String SELECT_LIVRO_BY_NOME = "SELECT * FROM livros WHERE nome = ?";
    private static final String SELECT_LIVRO_BY_ID = "SELECT * FROM livros WHERE id = ?";
    static Connection connection = null;

    // Recebe todos os livros do banco de dados, a query passada é através da:
    // private static final String SELECT_ALL_LIVROS
    public static List<Livro> getAllLivros() {
        List<Livro> livros = new ArrayList<>();
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        try {
            connection = BancoDados.ConexaoDb();

            preparedStatement = connection.prepareStatement(SELECT_ALL_LIVROS);
            resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                Livro livro = new Livro();
                livro.setId(resultSet.getInt("id"));
                livro.setNome(resultSet.getString("nome"));
                livro.setSinopse(resultSet.getString("sinopse"));
                livro.setGenero(resultSet.getString("genero"));
                livro.setCapa(resultSet.getString("capa"));
                livro.setArquivo_livro(resultSet.getString("arquivo_livro"));

                livros.add(livro);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            BancoDados.fecharConexao(connection);
        }

        return livros;
    }

    public static List<Livro> getLivroByGenero(String genero) {
        List<Livro> livrosList = new ArrayList<>();
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        try {
            connection = BancoDados.ConexaoDb();
            preparedStatement = connection.prepareStatement(SELECT_LIVRO_BY_GENERO);
            preparedStatement.setString(1, genero);

            resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                Livro livro = new Livro();

                livro.setId(resultSet.getInt("id"));
                livro.setNome(resultSet.getString("nome"));
                livro.setSinopse(resultSet.getString("sinopse"));
                livro.setGenero(resultSet.getString("genero"));
                livro.setCapa(resultSet.getString("capa"));
                livro.setArquivo_livro(resultSet.getString("arquivo_livro"));

                livrosList.add(livro);
            }

        } catch (SQLException e) {
            e.getErrorCode();
        } finally {
            BancoDados.fecharConexao(connection);
        }

        // Convert the list to an array and return it
        return livrosList;
    }

    public static List<Livro> getLivroByNome(String nomeLivro) {
        List<Livro> livros = new ArrayList<>();
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        try {
            connection = BancoDados.ConexaoDb();
            preparedStatement = connection.prepareStatement(SELECT_LIVRO_BY_NOME);
            preparedStatement.setString(1, nomeLivro);

            resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                Livro livro = new Livro();

                livro.setId(resultSet.getInt("id"));
                livro.setNome(resultSet.getString("nome"));
                livro.setSinopse(resultSet.getString("sinopse"));
                livro.setGenero(resultSet.getString("genero"));
                livro.setCapa(resultSet.getString("capa"));
                livro.setArquivo_livro(resultSet.getString("arquivo_livro"));

                livros.add(livro);
            }

        } catch (SQLException e) {
            e.getErrorCode();
        } finally {
            BancoDados.fecharConexao(connection);
        }

        return livros;
    }

    public static List<Livro> getLivroByID(String id) {
        List<Livro> livros = new ArrayList<>();
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        try {
            connection = BancoDados.ConexaoDb();
            preparedStatement = connection.prepareStatement(SELECT_LIVRO_BY_ID);
            preparedStatement.setInt(1, Integer.parseInt(id));

            resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                Livro livro = new Livro();

                livro.setId(resultSet.getInt("id"));
                livro.setNome(resultSet.getString("nome"));
                livro.setSinopse(resultSet.getString("sinopse"));
                livro.setGenero(resultSet.getString("genero"));
                livro.setCapa(resultSet.getString("capa"));
                livro.setArquivo_livro(resultSet.getString("arquivo_livro"));

                livros.add(livro);
            }

        } catch (SQLException e) {
            e.getErrorCode();
        } finally {
            BancoDados.fecharConexao(connection);
        }

        return livros;
    }
}