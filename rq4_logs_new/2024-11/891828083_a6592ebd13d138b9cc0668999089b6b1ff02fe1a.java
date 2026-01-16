package com.printsys.backend.controller.docs;

import com.printsys.backend.service.docs.AddDocsService;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class AddDocsController {

  @Autowired
  private AddDocsService addDocsService;

  @PostMapping("/docs/add/")
  public Map<String, String> add(@RequestParam Map<String, String> data) {
    return addDocsService.addDocs(data);
  }

}