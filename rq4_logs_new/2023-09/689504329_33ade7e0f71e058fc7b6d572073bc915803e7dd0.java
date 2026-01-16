package com.mega.biz.auth.controller;

import com.google.gson.Gson;
import com.mega.biz.auth.model.dto.AuthDTO;
import com.mega.biz.auth.model.dto.NameDTO;
import com.mega.biz.auth.model.dto.TokenDTO;
import com.mega.biz.auth.service.AuthService;
import com.mega.common.controller.ControllerUtils;
import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@WebServlet("/name")
public class NameController extends HttpServlet {
  AuthService service = new AuthService();

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    String body = ControllerUtils.getBody(req);
    Gson gson = new Gson();
    AuthDTO authDTO = gson.fromJson(body, AuthDTO.class);
    NameDTO nameDTO = service.getName(authDTO.getRefresh());
    String json = gson.toJson(nameDTO);

    resp.setStatus(200);
    resp.getWriter().write(json);
  }
}