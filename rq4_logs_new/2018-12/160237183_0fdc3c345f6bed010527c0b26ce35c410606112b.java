/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.edu.ifrn.web.bean;

import br.edu.ifrn.web.controle.CursoControle;
import br.edu.ifrn.web.modelo.Curso;
import javax.enterprise.inject.Model;
import javax.faces.application.FacesMessage;
import javax.faces.context.FacesContext;
import javax.inject.Inject;

/**
 *
 * @author beatriz
 */
@Model
public class CursoBean {

    @Inject
    private CursoControle cursoDAO;
    @Inject
    private Curso cursomodel;
    @Inject
    private FacesContext facesContext;

    private int idbeanCurso;

    public CursoControle getCursoDAO() {
        return cursoDAO;
    }

    public void setCursoDAO(CursoControle cursoDAO) {
        this.cursoDAO = cursoDAO;
    }

    public Curso getCursomodel() {
        return cursomodel;
    }

    public void setCursomodel(Curso cursomodel) {
        this.cursomodel = cursomodel;
    }

    public FacesContext getFacesContext() {
        return facesContext;
    }

    public void setFacesContext(FacesContext facesContext) {
        this.facesContext = facesContext;
    }

    public int getIdbeanCurso() {
        return idbeanCurso;
    }

    public void setIdbeanCurso(int idbeanCurso) {
        this.idbeanCurso = idbeanCurso;
    }

    public String salvarCurso() {
        String path = "/curso/";
        String mensagem;

        if (cursomodel.getId() != null) {
            cursoDAO.atualizarCurso(cursomodel);
            mensagem = "Curso Atuaizado com sucesso";
        } else {
            cursoDAO.salvarCurso(cursomodel);
            mensagem = "Curso Salvo com sucesso";
        }
        facesContext.getExternalContext().getFlash().setKeepMessages(true);
        facesContext.addMessage(null, new FacesMessage(mensagem));
        return "curso.xhtml";
    }

}