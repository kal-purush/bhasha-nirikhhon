package com.vcode.controller;

import com.vcode.Impl.VUserDaoImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.logging.Logger;

/**
 * @author moyee
 * @version 1.0.0
 * @Description vuser admin controller
 * @Date 2020.02.26
 */
@RestController
@RequestMapping("/admin/user")
public class VUserAdminController {

  @Autowired
  private VUserDaoImpl userDao;

  private Logger log = Logger.getLogger("UserAdminController");

  // TODO: write some api need admin permission
}