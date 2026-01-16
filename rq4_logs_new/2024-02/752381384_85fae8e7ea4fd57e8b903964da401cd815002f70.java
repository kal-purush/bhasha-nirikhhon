package com.example.controllers;

import com.example.repository.PhraseStorage;
import com.example.utils.beans.factory.BeanFactorySupportService;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.OngoingStubbing;

import java.io.*;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class HelpControllerTest {

    @Mock
    private HttpServletRequest request;
    @Mock
    private HttpServletResponse response;

    private ControllerSupportService controller;
    private StringWriter responseWriter;
    private PhraseStorage phraseStorage;
    private BeanFactorySupportService beanFactory;

    @BeforeEach
    void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        beanFactory = new BeanFactorySupportService();
        beanFactory.fillSingletonsMap("com.example");
        beanFactory.fillAutowired();
        beanFactory.wrapControllersWithLoggingProxy();

        controller = (ControllerSupportService) beanFactory.getBeans("helpController");
        phraseStorage = (PhraseStorage) beanFactory.getBeans("phraseStorage");

        responseWriter = new StringWriter();
        when(response.getWriter()).thenReturn(new PrintWriter(responseWriter));

        phraseStorage.clearPhrases();
    }

    @Test
    void doGetWithNoPhrases() throws Exception {
        when(request.getMethod()).thenReturn("GET");
        controller.handleRequest(request, response);
        verify(response).sendError(HttpServletResponse.SC_NOT_FOUND, "{\"error\":\"No phrases available.\"}");
    }

    @Test
    void doGetWithPhrases() throws Exception {
        phraseStorage.addPhrase("Test phrase");
        when(request.getMethod()).thenReturn("GET");
        controller.handleRequest(request, response);
        assertTrue(responseWriter.toString().contains("Test phrase"));
    }

    //Тесты для POST
}