/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package actions.authors;

import com.google.inject.Inject;
import com.opensymphony.xwork2.ActionSupport;
import facade.AuthorFacadeLocal;
import models.Author;

/**
 *
 * @author sergio
 */
public class CreateAuthorAction extends ActionSupport {
    @Inject
    private AuthorFacadeLocal authorFacade;
    private Author author = new Author();

    public Author getAuthor() {
        return author;
    }

    public void setAuthor(Author author) {
        this.author = author;
    }
    
    public String execute() throws Exception{
        authorFacade.create(author);
        addActionMessage(getText("success.author.create"));
        return SUCCESS;
    }
}