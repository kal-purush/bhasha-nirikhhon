/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.edu.ifrn.web.bean;

;
import br.edu.ifrn.web.controle.AlunoControle;
import br.edu.ifrn.web.modelo.Aluno;
import javax.enterprise.inject.Model;
import javax.faces.application.FacesMessage;
import javax.faces.context.FacesContext;
import javax.inject.Inject;
import javax.servlet.http.Part;

/**
 *
 * @author beatriz
 */

@Model
public class AlunoBean {

    @Inject
    private AlunoControle alunoDao;
    @Inject
    private Aluno alunomodel;
    @Inject
    private FacesContext facesContext;
    
    private Part fotoaluno;

    private int idbean;
    

    public AlunoControle getAlunoDao() {
        return alunoDao;
    }

    public void setAlunoDao(AlunoControle alunoDao) {
        this.alunoDao = alunoDao;
    }

    public Aluno getAlunomodel() {
        return alunomodel;
    }

    public void setAlunomodel(Aluno alunomodel) {
        this.alunomodel = alunomodel;
    }

    public FacesContext getFacesContext() {
        return facesContext;
    }

    public void setFacesContext(FacesContext facesContext) {
        this.facesContext = facesContext;
    }

    public int getIdbean() {
        return idbean;
    }

    public void setIdbean(int idbean) {
        this.idbean = idbean;
    }

    public Part getFotoaluno() {
        return fotoaluno;
    }

    public void setFotoaluno(Part fotoaluno) {
        this.fotoaluno = fotoaluno;
    }
    
    

public String salvarAluno(){
   String path = "/alunos/"  + fotoaluno.getSubmittedFileName() ;
   String mensagem;
   if(alunomodel.getId() != null){
       alunoDao.atualizar(alunomodel);
       mensagem = "Aluno Atualizado com suceso";
   }else{
       alunoDao.salvar(alunomodel);
       mensagem = "Aluno salvo com sucesso";
   }
   
   alunomodel = new Aluno();
   facesContext.getExternalContext().getFlash().setKeepMessages(true);
   facesContext.addMessage(null,new FacesMessage(mensagem));
   
   return "aluno.xhtml";

}
    
    
}