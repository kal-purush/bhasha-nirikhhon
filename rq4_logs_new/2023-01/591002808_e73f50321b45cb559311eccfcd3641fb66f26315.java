package com.example.demo;

import com.example.demo.domain.Comment;
import com.example.demo.service.CommentService;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


@SpringBootApplication
public class DemoApplication {

	private static CommentService commentService;

	public DemoApplication(CommentService commentService) {
		DemoApplication.commentService = commentService;
	}

	public static void main(String[] args) {
		var demo = SpringApplication.run(DemoApplication.class, args);

		Comment comment = commentService.findComment(1L);
		System.out.println(comment.toString());

		demo.close();
	}
}