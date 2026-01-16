/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fr.sysdev.softcpa.Controller.Invoice;

import fr.sysdev.softcpa.Service.IInvoiceService;
import fr.sysdev.softcpa.View.Invoice.InvoiceView;
import java.awt.event.ActionListener;
import javax.annotation.PostConstruct;
import javax.swing.JButton;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;

/**
 *
 * @author f
 */
@Controller
public class InvoiceController {

    @Autowired
    private final IInvoiceService invoiceService;
    @Autowired
    private final InvoiceView view;

   @PostConstruct
    private void prepareListeners() {
        
        //registerAction(view.getRefreshBtn(), (e) -> loadingClients());

    } 
    

    public InvoiceController(IInvoiceService invoiceService) {
        this.invoiceService = invoiceService;
        this.view = new InvoiceView(invoiceService.getInvoices()); 
         prepareListeners() ;
    }

    public InvoiceView getView() {
        return view;
    }

    public void loadingInvoices(){
    this.view.setInvoices(invoiceService.getInvoices());
    this.view.loadInvoices() ;
    }
 
    
    
    protected void registerAction(JButton button, ActionListener listener) {
        button.addActionListener(listener);
    }

  
}