/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package actions.books;

import com.google.inject.Inject;
import com.opensymphony.xwork2.ActionSupport;
import facade.BookFacadeLocal;
import models.Book;

/**
 *
 * @author sergio
 */
public class PostPersistBookAction extends ActionSupport {
    
    @Inject
    private BookFacadeLocal bookFacade;
    private Book book;

    public Book getBook() {
        return book;
    }

    public void setBook(Book book) {
        this.book = book;
    }
   
    public String execute() throws Exception{
        addActionMessage(getText("success.book.save"));
        return SUCCESS;
    }
}