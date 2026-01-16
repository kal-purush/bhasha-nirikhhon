/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Biblioteca.src;

import java.util.ArrayList;

public class Estante implements SujeitoObservavel{
    
    Livro livro;
    private ArrayList observadores;
    // private Livro livros[] = new Livro[8];
    private static Estante instancia;
       
    
    private Estante(){
        /*this.livros[0] = new Livro("100", "Engenharia de Software", "AddisonWesley", "Ian Sommerville", "6", "2000");
        this.livros[1] = new Livro("101", "UML - Guia do Usuario", "Campus", "Grady Booch, James Rumbaugh, Ivar Jacobson", "7", "2000");
        this.livros[2] = new Livro("200", "Code Complete", "Microsoft Press", "Steve McConnel", "2", "2014");
        this.livros[3] = new Livro("201", "Agile Software Development, Principles, Patterns and Practices", "Prentice Hall", "Robert Martin", "1", "2002");
        this.livros[4] = new Livro("300", "Refactoring: Improving the Design of Existing Code", "Addison-Wesley Professional", "Martin Fowler", "1", "1999");
        this.livros[5] = new Livro("301", "Software Metrics: Rigorous and Practical Approach", "CRC Press", "Norman Fenton, James Bieman", "3", "2014");
        this.livros[6] = new Livro("400", "Design Patterns: Elements of Reusable Object-Oriented Software", "Addison-Wesley Professional", "Erich Gamma Richard Helm, Ralph Johnson, John Vlissides", "1", "1994");
        this.livros[7] = new Livro("401", "UML Distilled: A Brief Guide to the Standart Object Modeling Language", "Addinson-Wesley Professional", "Martin Fowler", "3", "2003");
        */
    }
    
    public static Estante obterInstancia(){
        if (instancia == null){
            instancia = new Estante();
        }
        return instancia;
    }
    
    
    @Override
    public void addObservador(Usuario usuario) {
        observadores.add(usuario);
    }

    @Override
    public void removerObservador(Usuario usuario) {
        int i = observadores.indexOf(usuario);
		if (i >= 0) {
			observadores.remove(i);
                }
    }

    @Override
    public void notificarObservador() {
        for (int i = 0; i < observadores.size(); i++) {
            Usuario usuario = (Usuario)observadores.get(i);
		usuario.update();
	}
    }
    
}