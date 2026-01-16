//package com.mega.common.filter;
//
//import com.google.gson.Gson;
//import com.google.gson.stream.JsonToken;
//import com.mega.biz.login.model.LoginDAO;
//import com.mega.biz.login.model.dto.LoginDTO;
//import com.mega.common.jwt.Jwt;
//import java.io.BufferedReader;
//import java.io.IOException;
//import java.util.HashMap;
//import java.util.Map;
//import javax.servlet.Filter;
//import javax.servlet.FilterChain;
//import javax.servlet.ServletException;
//import javax.servlet.ServletRequest;
//import javax.servlet.ServletResponse;
//import javax.servlet.annotation.WebFilter;
//import javax.servlet.http.HttpFilter;
//import javax.servlet.http.HttpServletRequest;
//import javax.servlet.http.HttpServletResponse;
//
//@WebFilter(urlPatterns = {"/home/profile", "/home/attendance", "/home/curriculum", "/home/notice"})
//public class TokenCheckFilter extends HttpFilter implements Filter {
//
//  private final static int TOKEN_INDEX = 1;
//  private final static int ONE_DAY = 86400000;
//  LoginDAO dao = new LoginDAO();
//
//  @Override
//  public void doFilter(ServletRequest req, ServletResponse res, FilterChain chain)
//      throws IOException, ServletException {
//    HttpServletRequest request = (HttpServletRequest) req;
//    HttpServletResponse response = (HttpServletResponse) res;
//
//    String[] authorization = request.getHeader("Authorization").split(" ");
//    String tokenKind = request.getHeader("Token-Kind");
//
//    String token = authorization[TOKEN_INDEX];
//    Gson gson = new Gson();
//
//    try {
//      if (tokenKind.equals("access") && Jwt.validateIssuer(token) && Jwt.validateToken(token)) {
//        response.setStatus(201);
//        response.getWriter().write("");
//        chain.doFilter(request, response);
//        return;
//      } else if (tokenKind.equals("refresh") && Jwt.validateIssuer(token) && Jwt.validateToken(
//          token)) {
//        try {
//          if (dao.selectRefresh(Jwt.validateEmail(token)).getRefresh().equals(token)) {
//            response.setStatus(201);
//            Map<String, String> tokenMap = new HashMap<>();
//            tokenMap.put("access",
//                Jwt.create(Jwt.validateEmail(token), Jwt.validateName(token), 600000));
//
//            if (Jwt.validateExp(token) <= ONE_DAY) {
//              tokenMap.put("refresh",
//                  Jwt.create(Jwt.validateEmail(token), Jwt.validateName(token), 3600000 * 24 * 14));
//              dao.renewRefresh(tokenMap.get("refresh"), Jwt.validateEmail(token));
//            }
//
//            request.setAttribute("token", tokenMap);
//          }
//        } catch (Exception e) {
//          response.setStatus(401);
//          response.getWriter().write("");
//          return;
//        }
//      }
//    } catch (Exception e) {
//      System.out.println("!!!!!");
//      response.setStatus(401);
//      response.getWriter().write("");
//      return;
//    }
//
//    chain.doFilter(request, response);
//  }
//}