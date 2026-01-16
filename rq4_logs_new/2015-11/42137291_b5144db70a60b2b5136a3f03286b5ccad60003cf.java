package com.senac.biblioteca.old;

/**
 * Classa para gerar ids dos livros, usuários e emprestimo
 * @author ruans
 */
public class GeraId {
    
    public static int idLivro      = 0;
    public static int idUsuario    = 0;    
    public static int idEmprestimo = 0;

    /*
    * ++ usado para adicionar 1 à variável a e depois calcular a expressão na qual a reside;
    */
    
    public static int getIdLivro() {
        return ++idLivro;
    }

    public static int getIdUsuario() {
        return ++idUsuario;
    }

    public static int getIdEmprestimo() {
        return ++idEmprestimo;
    }
    
    
}