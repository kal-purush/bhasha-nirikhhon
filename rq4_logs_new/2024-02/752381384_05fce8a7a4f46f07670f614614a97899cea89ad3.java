package com.example.servlets;

import com.example.controllers.ControllerSupportService;
import com.example.utils.beans.factory.BeanFactorySupportService;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;

import static org.mockito.Mockito.*;

public class DispatcherServletSupportServiceTest {

    @Mock
    private HttpServletRequest request;
    @Mock
    private HttpServletResponse response;
    @Mock
    private BeanFactorySupportService beanFactory;
    @Mock
    private ControllerSupportService helpController;

    private DispatcherServletSupportService servlet;

    @BeforeEach
    void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        servlet = new DispatcherServletSupportService();
        beanFactory = mock(BeanFactorySupportService.class);
        helpController = mock(ControllerSupportService.class);

        when(beanFactory.getBeans("helpController")).thenReturn(helpController);

        servlet.init();

    }

    @Test
    void testInvalidRequest() throws ServletException, IOException {
        String invalidUri = "/invalid/path";
        when(request.getRequestURI()).thenReturn(invalidUri);
        when(request.getContextPath()).thenReturn("");

        servlet.service(request, response);

        verify(response).sendError(HttpServletResponse.SC_NOT_FOUND, "Not found");
    }
}