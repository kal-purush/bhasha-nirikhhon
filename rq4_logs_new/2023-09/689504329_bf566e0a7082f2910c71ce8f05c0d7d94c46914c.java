package com.mega.biz.home.controller;

import com.google.gson.Gson;
import com.mega.biz.home.model.dto.HomeCurriculumDTO;
import com.mega.biz.home.service.HomeService;
import java.io.IOException;
import java.util.ArrayList;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

  @WebServlet("/home/curriculum")
public class HomeCurriculumController extends HttpServlet {
  private final HomeService service = new HomeService();
  Gson gson = new Gson();

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    ArrayList<HomeCurriculumDTO> homeCurriculumListDTO = service.getRecentCurriculum();

    String result = gson.toJson(homeCurriculumListDTO);

    response.setStatus(200);
    response.getWriter().write(result);
  }
}