package org.restsql.service;

import org.json.simple.JSONObject;
import org.restsql.core.impl.serial.JsonResponseSerializer;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.security.Principal;

public class ProfileServlet extends HttpServlet {
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {

        Principal p = request.getUserPrincipal();

        JSONObject o = new JSONObject();

        o.put("user", p.getName());

        response.getOutputStream().print( o.toJSONString());

    }
}