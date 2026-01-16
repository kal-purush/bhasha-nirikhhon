package com.vcode.controller;

import com.vcode.common.ResponseCode;
import com.vcode.entity.Response;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author moyee
 * @version 1.0.0
 * @Description
 * @Date
 */
@RestController
public class WebController {

  @RequestMapping("/401")
  @ResponseStatus(HttpStatus.UNAUTHORIZED)
  public Response handle401() {
    return new Response(ResponseCode.FAIL, "Unauthorized", null, null);
  }
}