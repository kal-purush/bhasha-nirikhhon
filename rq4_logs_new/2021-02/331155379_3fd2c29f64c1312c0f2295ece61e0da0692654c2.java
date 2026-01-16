package com.example.capstoneweb.Controller;

import com.example.capstoneweb.model.Tag;
import com.example.capstoneweb.service.TagService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@CrossOrigin(origins = "http://localhost:3000")
@RestController
@RequestMapping("/api/board")
public class TagController {
    @Autowired
    private TagService tagService;

    //get all tag
    @GetMapping("/tag")
    public List<Tag> getAllTag(){
        return tagService.getAllTag();
    }
    //게시판 글에 관련된 태그
    @GetMapping("/tag/{num}")
    public ResponseEntity<Tag> getTagByNum(@PathVariable Integer num){
        return tagService.getTag(num);
    }

}