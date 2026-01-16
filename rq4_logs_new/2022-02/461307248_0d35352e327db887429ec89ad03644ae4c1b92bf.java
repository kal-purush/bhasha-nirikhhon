package com.sip.springdata1.controllers;

import javax.validation.Valid;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import com.sip.springdata1.entities.Article;
import com.sip.springdata1.repository.ArticleRepository;

@Controller
@RequestMapping("/article/")
public class ArticleController {



   private final ArticleRepository articleRepository ;

   @Autowired
   public ArticleController(ArticleRepository articleRepository) {
	super();
	this.articleRepository = articleRepository;
}
   
	@GetMapping("list")
	public String listarticles(Model model) {
		model.addAttribute("articles",this.articleRepository.findAll());
		return "article/listArticles";
	}
	
	@GetMapping("add")
	public String addarticleget(Model model) {
		Article article = new Article();
		model.addAttribute("article",article);
		return"article/addArticle";
	}
	
	@PostMapping("add")
	public String addarticlepost(Model model,@Valid Article article,BindingResult result) {
		if(result.hasErrors()) {
			return"article/addArticle";
		}
		articleRepository.save(article);
		return"redirect:/article/list";
	}
	
	
	@GetMapping("update/{id}")
	public String updatearticleget(Model model,@PathVariable("id") long id) {
		Article article = articleRepository.findById(id).orElseThrow(()-> new IllegalArgumentException("Invalid article Id:" + id));
		model.addAttribute("article",article);
		return"article/updateArticle";
	}
	
	@PostMapping("update")
	public String updatearticlepost(Model model,@Valid Article article,BindingResult result) {
		
		articleRepository.save(article);
		return"redirect:list";
	}
	
	@GetMapping("delete/{id}")
	public String deletearticleget(Model model,@PathVariable("id") long id) {
		Article article = articleRepository.findById(id).orElseThrow(()-> new IllegalArgumentException("Invalidarticle Id:" + id));
		articleRepository.delete(article);
		return"redirect:../list";
	}
}