package com.autohome.api.controller;

import com.autohome.api.service.ForumService;
import com.autohome.common.vo.R;
import com.autohome.entity.ForumFavorite;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("api/forum/")
public class ForumController {
    @Autowired
    private ForumService forumService;
    @GetMapping("selectForumByName.do")
    public R selectForumByName(String forum_name){
        return forumService.selectForumByName(forum_name);
    }

    @GetMapping("selectAllForum.do")
    public R selectAllForum(){
        return forumService.selectAllForum();
    }

    @PostMapping("addForum_favorite.do/{forumFavorite}")
    public R addForum_favorite(@PathVariable ForumFavorite forumFavorite){
        return forumService.addForum_favorite(forumFavorite);
    }
}