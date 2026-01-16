package com.mega.biz.auth.controller;

import com.google.gson.Gson;
import com.mega.biz.auth.model.dto.AuthDTO;
import com.mega.biz.auth.service.AuthService;
import com.mega.biz.login.exception.TokenException;
import com.mega.common.jwt.Jwt;
import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@WebServlet("/auth")
public class AuthController extends HttpServlet {
  AuthService service = new AuthService();

  private final static int TOKEN_INDEX = 1;
  private final static int ONE_DAY = 86400000;
  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    String[] queries = req.getHeader("Authorization").split("Bearer ");
    String tokenKind = req.getHeader("Token-Kind");
    String token = queries[TOKEN_INDEX];

    AuthDTO authDTO = new AuthDTO();
    Gson gson = new Gson();

    if(tokenKind.equals("access")) {
      try {
        service.validateAccessToken(token);
        resp.setStatus(200);
        resp.getWriter().write("");
      } catch(TokenException e) {
        resp.setStatus(401);
//        String json = gson.toJson(authDTO);
//        resp.getWriter().write(json);
        resp.getWriter().write("");
      }
    } else if(tokenKind.equals("refresh")) {
      try {
        String email = Jwt.getTokenInnerEmail(token);
        String name = Jwt.getTokenInnerName(token);

        service.validateAccessToken(token);
        service.getNewAccess(authDTO, email, name, 600000);

        if(Jwt.validateExp(token) <= ONE_DAY) {
          service.getNewRefresh(authDTO, email, name, 3600000 * 24 * 14);
        }

        String json = gson.toJson(authDTO);
        resp.setStatus(200);
        resp.getWriter().write(json);
      } catch(TokenException e) {
        resp.setStatus(401);
//        String json = gson.toJson(authDTO);
//        resp.getWriter().write(json);
        resp.getWriter().write("");
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}