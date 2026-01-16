/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package actions.books;

import com.google.inject.Inject;
import com.opensymphony.xwork2.ActionSupport;
import com.opensymphony.xwork2.Preparable;
import facade.BookFacadeLocal;
import models.Book;

/**
 *
 * @author sergio
 */
public class BookAction extends ActionSupport implements Preparable {
    
    @Inject
    private BookFacadeLocal bookFacade;
    private Integer idBook;
    private Book book;

    public Integer getIdBook() {
        return idBook;
    }

    public void setIdBook(Integer idBook) {
        this.idBook = idBook;
    }

    public Book getBook() {
        return book;
    }

    public void setBook(Book book) {
        this.book = book;
    }
    
    public String execute() throws Exception{
        return SUCCESS;
    }
    
    public String persist() throws Exception{
        addActionMessage(getText("success.book.save"));
        return SUCCESS;
    }

    @Override
    public void prepare() throws Exception {
        if(idBook != null){
            book = bookFacade.find(idBook);
        }else{
            book = new Book();
        }
    }
}