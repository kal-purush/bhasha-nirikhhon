package com.cartas.jaktani.controller;

import com.cartas.jaktani.dto.CommentDto;
import com.cartas.jaktani.exceptions.ResourceNotFoundException;
import com.cartas.jaktani.service.CommentService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;


@RestController
@RequestMapping(path = "/api/comment")
public class CommentController {
    @Autowired
    CommentService commentService;

    @GetMapping(path = "/getById/{id}")
    public Object getCommentByID(@PathVariable(name = "id") Integer id) throws ResourceNotFoundException {
        return commentService.getCommentByID(id);
    }

    @GetMapping(path = "/all")
    public Object getAllComment() {
        return commentService.getAllComments();
    }
    
    @GetMapping(path = "/allByProductId/{productId}")
    public Object getAllByProductId(@PathVariable(name = "productId") Integer productId) {
        return commentService.getAllCommentByProductId(productId);
    }
    
    @GetMapping(path = "/allByShopId/{shopId}")
    public Object getAllByShopId(@PathVariable(name = "shopId") Integer shopId) {
        return commentService.getAllCommentByShopId(shopId);
    }

	@PostMapping(path = "/save") 
	public Object addComment(@RequestBody CommentDto comment) { 
		return commentService.addComment(comment); 
	}

    @PostMapping(path = "/delete/{id}")
    public Object deleteCommentByID(@PathVariable(name = "id") Integer id) throws ResourceNotFoundException {
        return commentService.deleteCommentByID(id);
    }

}