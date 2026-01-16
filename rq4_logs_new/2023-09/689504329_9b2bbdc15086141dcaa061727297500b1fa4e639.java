package com.mega.biz.home.controller;

import com.google.gson.Gson;
import com.mega.biz.home.model.dto.HomeAttendanceDTO;
import com.mega.biz.home.service.HomeService;
import com.mega.common.controller.Controller;

import java.io.IOException;
import java.util.ArrayList;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@WebServlet("/home")
public class HomeController extends HttpServlet {

  private final HomeService service = new HomeService();
  Gson gson = new Gson();

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    String name = request.getHeader("userName");
    System.out.println(name);
    ArrayList<HomeAttendanceDTO> homeAttendanceListDTO = service.getAttendanceStat(name);
    String result = gson.toJson(homeAttendanceListDTO);

    response.setStatus(200);
    response.getWriter().write(result);
  }
}