package com.film.bank;

import com.film.bank.service.FilmDataService;
import com.vaadin.server.VaadinServlet;

import javax.ejb.EJB;
import javax.servlet.annotation.WebInitParam;
import javax.servlet.annotation.WebServlet;

/**
 * Created by Stepan on 1/10/2016.
 */
@WebServlet(name = "FilmBankServlet", urlPatterns = "/*",
        initParams = {@WebInitParam(name = "UI", value = "com.film.bank.FilmBankUI")
        })
public class FilmBankServlet extends VaadinServlet {
    @EJB
    private FilmDataService userDataService;

    public FilmDataService getUserDataService() {
        return userDataService;
    }
}