/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package facade;

import java.util.List;
import javax.ejb.Stateless;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import models.PdfFile;

/**
 *
 * @author sergio
 */
@Stateless
public class FileStorageFacade extends AbstractFacade<PdfFile> implements FileStorageFacadeLocal {

    @PersistenceContext(unitName = "ejercicio4PU")
    private EntityManager em;

    @Override
    protected EntityManager getEntityManager() {
        return em;
    }

    public FileStorageFacade() {
        super(PdfFile.class);
    }
}