/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package actions.books;

import actions.FileProcessAction;
import com.google.inject.Inject;
import com.opensymphony.xwork2.Preparable;
import facade.AuthorFacadeLocal;
import facade.BookFacadeLocal;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import models.Author;
import models.Book;
import models.Level;
import java.net.URI;
import org.apache.struts2.interceptor.SessionAware;

/**
 *
 * @author sergio
 */
public class PersistBookAction extends FileProcessAction implements Preparable, SessionAware{
    
    private final static String CURRENT_BOOK = "current_book";
    @Inject
    private BookFacadeLocal bookFacade;
    @Inject
    private AuthorFacadeLocal authorFacade;
    private Long idBook;
    private Book book = new Book();
    private Level[] levels;
    private List<Author> authors;
    private Map<String, Object> session;

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

    public models.Level[] getLevels() {
        levels = Level.values();
        return levels;
    }

    public void setLevels(models.Level[] levels) {
        this.levels = levels;
    }

    public List<Author> getAuthors() {
        authors = authorFacade.findAll();
        return authors;
    }

    public void setAuthors(List<Author> authors) {
        this.authors = authors;
    }
   
    public String execute() throws Exception {
        return SUCCESS;
    }
   
    public String proccess() throws Exception {
        session.put(CURRENT_BOOK, book);
        if (upload != null) {
            URI uriFile = uploadFile("excerpt_folder");
            book.setExcerpt(uriFile);
        }
        if(book.getId() != null){
            Logger.getLogger(PersistBookAction.class.getName()).log(java.util.logging.Level.INFO, "UPDATE BOOK: " + book.toString());
            bookFacade.edit(book);
        }else{
            Logger.getLogger(PersistBookAction.class.getName()).log(java.util.logging.Level.INFO, "SAVE BOOK: " + book.toString());
            bookFacade.create(book);
        }
        addActionMessage(getText("success.book.save"));
        session.remove(CURRENT_BOOK);
        return SUCCESS;
    }

    @Override
    public void setSession(Map<String, Object> map) {
        this.session = map;
    }
    
    @Override
    public void prepare() throws Exception {
        if(idBook != null){
            Logger.getLogger(PersistBookAction.class.getName()).log(java.util.logging.Level.INFO, "ID BOOK: " + idBook);
            book = bookFacade.find(idBook);
        }else if (session.containsKey(CURRENT_BOOK)){
            book = (Book) session.get(CURRENT_BOOK);
        } else {
            book = new Book();
        }
    }
}