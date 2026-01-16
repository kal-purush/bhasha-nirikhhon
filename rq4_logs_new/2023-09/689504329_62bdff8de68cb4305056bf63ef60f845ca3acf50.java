package com.mega.biz.curriculum.controller;

import com.google.gson.Gson;
import com.mega.biz.curriculum.model.dto.CurriculumWithDetailDTO;
import com.mega.biz.curriculum.service.CurriculumService;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;

@WebServlet("/curriculum")
public class CurriculumController extends HttpServlet {

    private final CurriculumService service = new CurriculumService();
    Gson gson = new Gson();

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        List<CurriculumWithDetailDTO> allCurriculumWithDetail = service.getAllCurriculumWithDetail();
        String result = gson.toJson(allCurriculumWithDetail);

        response.getWriter().write(result);
    }
}