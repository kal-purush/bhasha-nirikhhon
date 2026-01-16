package com.mega.biz.notice.controller;

import com.google.gson.Gson;
import com.mega.biz.notice.model.dto.NoticeDTO;
import com.mega.biz.notice.service.NoticeService;
import com.mega.common.controller.Controller;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;

@WebServlet("/notice")
public class NoticeController extends HttpServlet {

    private final NoticeService service = new NoticeService();
    Gson gson = new Gson();

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        List<NoticeDTO> noticeList = service.getNoticeList();

        String json = gson.toJson(noticeList);

        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().write(json);
    }
}