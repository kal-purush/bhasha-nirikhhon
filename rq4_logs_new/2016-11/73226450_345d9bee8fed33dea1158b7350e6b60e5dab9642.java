/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package actions.books;

import actions.BaseAction;
import com.google.inject.Inject;
import com.opensymphony.xwork2.Preparable;
import facade.BookFacadeLocal;
import models.Book;

/**
 *
 * @author sergio
 */
public class RemoveBookAction extends BaseAction implements Preparable {
    
    @Inject
    private BookFacadeLocal bookFacade;
    private Book book = null;
    private Long idBook;

    public Long getIdBook() {
        return idBook;
    }

    public void setIdBook(Long idBook) {
        this.idBook = idBook;
    }

    public Book getBook() {
        return book;
    }

    public void setBook(Book book) {
        this.book = book;
    }
    
    public String execute() throws Exception {
        if(book != null){
            bookFacade.remove(book);
            addActionMessage(getText("success.book.remove"));
            return SUCCESS;
        } else {
            this.addActionError(getText("errors.books.notfound"));
            return INPUT;
        }
        
    }

    @Override
    public void prepare() throws Exception {
        if(idBook != null){
            book = bookFacade.find(idBook);
        }
    }
}