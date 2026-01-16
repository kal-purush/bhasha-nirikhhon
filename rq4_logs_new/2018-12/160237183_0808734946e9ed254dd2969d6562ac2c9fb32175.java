/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.edu.ifrn.web.bean;

import br.edu.ifrn.web.controle.ProfControle;
import br.edu.ifrn.web.modelo.Professor;
import java.util.List;
import javax.enterprise.inject.Model;
import javax.faces.application.FacesMessage;
import javax.faces.context.FacesContext;
import javax.inject.Inject;

/**
 *
 * @author beatriz
 */
@Model
public class ProfBean {
    @Inject
    private ProfControle profDAO;
    @Inject
    private Professor profmodel;
    @Inject
    private FacesContext facesContext;
    
    private List<Professor> listaProf;
    
    private Integer idProfBean;

    public ProfControle getProfDAO() {
        return profDAO;
    }

    public void setProfDAO(ProfControle profDAO) {
        this.profDAO = profDAO;
    }

    public Professor getProfmodel() {
        return profmodel;
    }

    public void setProfmodel(Professor profmodel) {
        this.profmodel = profmodel;
    }

    public FacesContext getFacesContext() {
        return facesContext;
    }

    public void setFacesContext(FacesContext facesContext) {
        this.facesContext = facesContext;
    }

    public List<Professor> getListaProf() {
        return listaProf;
    }

    public void setListaProf(List<Professor> listaProf) {
        this.listaProf = listaProf;
    }

    public Integer getIdProfBean() {
        return idProfBean;
    }

    public void setIdProfBean(Integer idProfBean) {
        this.idProfBean = idProfBean;
    }
    
    
    public String salvarProf() {
        String path = "/professor/";
        String mensagem;
        if (profmodel.getId() != null) {
            profDAO.atualizar(profmodel);
            mensagem = "Professor Atualizado com suceso";
        } else {
            profDAO.salvar(profmodel);
            mensagem = "Professor salvo com sucesso";
        }

        profmodel = new Professor();
        facesContext.getExternalContext().getFlash().setKeepMessages(true);
        facesContext.addMessage(null, new FacesMessage(mensagem));

        return "professor.xhtml";

    }

    public void excluirProfessor(Professor professor) {
        profDAO.excluir(professor);
        profDAO = null;
    }

    public List<Professor> listarProfessor() {
        if (listaProf == null) {
            listaProf = profDAO.listar();
        }
        return listaProf;
    }
    
    
    public String atualizar(Integer id){
        return "professor.xhtml?id=" + String.valueOf(id);
    }
    
    
    public void carregarLivro(){
        if (idProfBean != null){
            Professor professor = profDAO.buscar(idProfBean);
            if (professor != null){
                this.profmodel = professor;
                return;
            }
        }             
    }
    
}