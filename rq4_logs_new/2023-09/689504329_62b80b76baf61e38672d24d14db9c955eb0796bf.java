package com.mega.biz.qr.controller;

import com.mega.biz.qr.exception.QrException;
import com.mega.biz.qr.exception.ReAuthException;
import com.mega.biz.qr.model.dto.QrDTO;
import com.mega.biz.qr.service.QrService;
import com.mega.common.controller.Controller;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class QrController implements Controller {

    private final QrService service = new QrService();

    @Override
    public Object handleRequest(HttpServletRequest request, HttpServletResponse response)  throws ReAuthException, QrException {
        String qr = request.getAttribute("qr").toString();
        String email = request.getHeader("email");

        QrDTO dto = new QrDTO(qr, email);

        return service.authQr(dto);
    }
}