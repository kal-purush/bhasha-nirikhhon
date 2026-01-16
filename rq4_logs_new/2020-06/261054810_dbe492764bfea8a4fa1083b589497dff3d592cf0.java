package br.com.codenation.v1.errorManager.controller;

import br.com.codenation.v1.errorManager.entity.Log;
import br.com.codenation.v1.errorManager.service.impl.LogService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/log")
public class LogController {

  private LogService logService;

  @Autowired
  public LogController(LogService logService) {
    this.logService = logService;
  }

  @GetMapping("/{id}")
  @ResponseStatus(HttpStatus.OK)
  public Optional<Log> findById(@PathVariable Long id){
    return logService.findById(id);
  }

  @GetMapping("/{applicationId}")
  @ResponseStatus(HttpStatus.OK)
  public List<Log> findByApplicationId(@PathVariable Long applicationId) {
    return logService.findByApplicationId(applicationId);
  }
}