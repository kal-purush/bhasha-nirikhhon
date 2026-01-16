package org.eurofurence.regsys.web.servlets.base;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.eurofurence.regsys.web.servlets.HttpMethod;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public abstract class BaseServlet extends HttpServlet {
    protected abstract void doAnyMethod(HttpServletRequest request, HttpServletResponse response, HttpMethod method);

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) {
        doAnyMethod(request, response, HttpMethod.GET);
    }

    protected void forceRequestEncoding(HttpServletRequest request) {
        try {
            request.setCharacterEncoding("UTF-8");
        } catch (UnsupportedEncodingException ignore) {
            // not happening
        }
    }

    @Override
    public void doPost(HttpServletRequest request, HttpServletResponse response) {
        forceRequestEncoding(request);
        doAnyMethod(request, response, HttpMethod.POST);
    }

    @Override
    public void doPut(HttpServletRequest request, HttpServletResponse response) {
        forceRequestEncoding(request);
        doAnyMethod(request, response, HttpMethod.PUT);
    }

    @Override
    public void doDelete(HttpServletRequest request, HttpServletResponse response) {
        forceRequestEncoding(request);
        doAnyMethod(request, response, HttpMethod.DELETE);
    }

    @Override
    public void doOptions(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        super.doOptions(request, response);
    }
}