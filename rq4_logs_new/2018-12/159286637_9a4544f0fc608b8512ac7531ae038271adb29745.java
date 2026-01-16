/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ecommerce.struts;

import com.opensymphony.xwork2.Action;
import ecommerce.entities.Orders;

/**
 *
 * @author PhuNDSE63159
 */
public class SendEmailAction {

    private Orders order;
    private String htmlText;

    public SendEmailAction() {
    }

    public String execute() throws Exception {
        String url = Action.SUCCESS;
        
        return url;
    }

    public Orders getOrder() {
        return order;
    }

    public void setOrder(Orders order) {
        this.order = order;
    }

    public String getHtmlText() {
        return htmlText;
    }

    public void setHtmlText(String htmlText) {
        this.htmlText = htmlText;
    }
    
    

}