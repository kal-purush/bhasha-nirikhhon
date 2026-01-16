package com.msg.data.controller;

import com.msg.data.model.ErrCode;
import com.msg.data.model.UserLogModel;
import com.msg.data.service.UserLogService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * Created by yoga.wiguna on 27/08/2018.
 */

@RestController
@RequestMapping("/login")
public class UserLogController {

    @Autowired
    private UserLogService userLogService;

    @RequestMapping(value = "/login", method = RequestMethod.POST)
    public ResponseEntity<Void> nikPasswd(@RequestBody UserLogModel userLogModel, UriComponentsBuilder uriComponentsBuilder) {
        if (userLogService.isNikExist(userLogModel.getNik(), userLogModel.getPasswd()) == true) {
            return new ResponseEntity(new ErrCode("201", "Berhasil Login : " ), HttpStatus.CREATED);
        }
        return new ResponseEntity(new ErrCode("404", "Data Kosong "), HttpStatus.NOT_FOUND);
    }

}