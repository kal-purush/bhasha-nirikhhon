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
public class BookAction extends BaseAction implements Preparable {
    
    @Inject
    private BookFacadeLocal booksFacade;
    private Book book;
    private Long idBook;

    public Book getBook() {
        return book;
    }

    public void setBook(Book book) {
        this.book = book;
    }
   
    public Long getIdBook() {
        return idBook;
    }

    public void setIdBook(Long idBook) {
        this.idBook = idBook;
    }
    
    public String execute() throws Exception {
        return SUCCESS;
    }

    @Override
    public void prepare() throws Exception {
       if(idBook != null){
           book = booksFacade.find(idBook);
       } else {
           book = new Book();
       }
    }
}