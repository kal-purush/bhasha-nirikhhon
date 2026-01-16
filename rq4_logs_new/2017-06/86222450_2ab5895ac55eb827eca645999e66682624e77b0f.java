package com.hotel.web.controller;

import com.hotel.service.RoomService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.commons.CommonsMultipartFile;

import java.io.File;
import java.io.IOException;

/**
 * Created by hp on 2017/5/31.
 */
@Controller
@RequestMapping("/files")
public class UploadController {
    @Autowired
    private RoomService roomService;
    @RequestMapping(value="/{room_id}/image.action",method = RequestMethod.POST)
    @ResponseBody
    public Object uploadImage(@RequestParam("image") CommonsMultipartFile file,@PathVariable("room_id") Integer roomId){
        return roomService.uploadImage(file,roomId);
    }
}