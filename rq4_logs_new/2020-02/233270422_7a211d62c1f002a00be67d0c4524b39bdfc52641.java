package com.vcode.controller;

import com.vcode.common.ResponseCode;
import com.vcode.entity.Response;
import com.vcode.util.StringHandler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.logging.Logger;

@RestController
@RequestMapping("/test_case")
public class TestCaseController {

  private Logger log = Logger.getLogger("TestCaseController");

  @PostMapping("/upload")
  public Response uploadTestCase(@RequestParam("file") MultipartFile file) {
    Response response = new Response();
    // check file is exist
    if (file.isEmpty()) {
      response.setCode(ResponseCode.FAIL);
      response.setMessage("please select one file to upload");
      return response;
    }
    // get test_case_id
    String testCaseId = StringHandler.generateRandomStr(16);
    // write file data
    try {
      byte[] bytes = file.getBytes();
      Path path = Paths.get("/tmp/" + testCaseId + ".zip");
      Files.write(path, bytes);
    } catch (IOException e) {
      e.printStackTrace();
      log.warning("write test case data error");
    }
    // return response
    HashMap<String, Object> resData = new HashMap<>();
    resData.put("testCaseId", testCaseId);
    response.setData(resData);
    return response;
  }
}