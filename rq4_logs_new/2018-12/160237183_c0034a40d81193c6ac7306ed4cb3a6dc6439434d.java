/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.edu.ifrn.web.bean;

import br.edu.ifrn.web.controle.AlunoControle;
import br.edu.ifrn.web.modelo.Aluno;
import java.util.List;
import javax.enterprise.inject.Model;

/**
 *
 * @author beatriz
 */
@Model
public class ListaAluno {

    private AlunoControle alunoControl;
    private List<Aluno> alunoList;

    public List<Aluno> listar() {
        if (alunoList == null) {
            alunoList = alunoControl.listar();
        }
        return alunoList;
    }

    
     public void excluir(Aluno aluno){
        alunoControl.excluir(aluno);
        alunoList = null;
    }
    
     
     public String atualizar(Integer id){
        return "aluno.xhtml?id=" + String.valueOf(id);
    }
     
     public String novo(){
        return "aluno.xhtml?faces-redirect=true";
    }
     
}