/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.edu.ifrn.web.bean;

import br.edu.ifrn.web.controle.DisOferControle;
import br.edu.ifrn.web.modelo.DisciplinaOfertada;
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
public class DisOferbean {

    @Inject
    private DisOferControle disoferDAO;

    @Inject
    private DisciplinaOfertada disOfermodel;

    @Inject
    private FacesContext facesContext;

    private List<DisciplinaOfertada> listaDis;

    private Integer idDisOfer;

    public DisOferControle getDisoferDAO() {
        return disoferDAO;
    }

    public void setDisoferDAO(DisOferControle disoferDAO) {
        this.disoferDAO = disoferDAO;
    }

    public DisciplinaOfertada getDisOfermodel() {
        return disOfermodel;
    }

    public void setDisOfermodel(DisciplinaOfertada disOfermodel) {
        this.disOfermodel = disOfermodel;
    }

    public FacesContext getFacesContext() {
        return facesContext;
    }

    public void setFacesContext(FacesContext facesContext) {
        this.facesContext = facesContext;
    }

    public List<DisciplinaOfertada> getListaDis() {
        return listaDis;
    }

    public void setListaDis(List<DisciplinaOfertada> listaDis) {
        this.listaDis = listaDis;
    }

    public Integer getIdDisOfer() {
        return idDisOfer;
    }

    public void setIdDisOfer(Integer idDisOfer) {
        this.idDisOfer = idDisOfer;
    }
    
    public String salvarDisOfer() {
        String path = "/disofer/";
        String mensagem;
        if (disOfermodel.getId() != null) {
            disoferDAO.atualizar(disOfermodel);
            mensagem = " Atualizado com suceso";
        } else {
            disoferDAO.salvar(disOfermodel);
            mensagem = " salvo com sucesso";
        }

        disOfermodel = new DisciplinaOfertada();
        facesContext.getExternalContext().getFlash().setKeepMessages(true);
        facesContext.addMessage(null, new FacesMessage(mensagem));

        return "disofer.xhtml";

    }

    public void excluirDisOfer(DisciplinaOfertada disofer) {
        disoferDAO.excluir(disofer);
        disoferDAO = null;
    }

    public List<DisciplinaOfertada> listarDisOfer() {
        if (listaDis == null) {
            listaDis = disoferDAO.listar();
        }
        return listaDis;
    }
    
    
    public String atualizar(Integer id){
        return "disofer.xhtml?id=" + String.valueOf(id);
    }
    
    
    public void carregarLivro(){
        if (idDisOfer != null){
            DisciplinaOfertada disofer = disoferDAO.buscar(idDisOfer);
            if (disofer != null){
                this.disOfermodel = disofer;
                return;
            }
        }             
    }

}