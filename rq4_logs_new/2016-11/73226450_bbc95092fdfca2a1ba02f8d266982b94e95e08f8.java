/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package actions.books;

import com.google.inject.Inject;
import com.opensymphony.xwork2.ActionSupport;
import facade.PdfFileFacadeLocal;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import models.PdfFile;

/**
 *
 * @author sergio
 */
public class ShowPDFAction extends ActionSupport{
    
    @Inject
    private PdfFileFacadeLocal pdfsFacade;
    private Long idPdf;
    private InputStream inputStream;
   
    public Long getIdPdf() {
        return idPdf;
    }

    public void setIdPdf(Long idPdf) {
        this.idPdf = idPdf;
    }

    public InputStream getInputStream() {
        return inputStream;
    }

    public void setInputStream(InputStream inputStream) {
        this.inputStream = inputStream;
    }
    
    public String execute() throws Exception {
        PdfFile pdf = pdfsFacade.find(idPdf);
        inputStream = new ByteArrayInputStream(pdf.getContent()); 
        return SUCCESS;
    }
}