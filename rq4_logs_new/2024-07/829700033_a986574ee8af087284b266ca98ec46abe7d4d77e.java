package com.generation.blogpessoal.model;
import java.time.LocalDateTime;

import org.hibernate.annotations.UpdateTimestamp;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

@Entity
@Table(name = "tb_posts")
public class Post {
	
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;
	
	@NotBlank(message = "O atributo TITULO é Obrigatorio!")
	@Size(min = 3, max = 100, message = "O atributo TITULO deve conter no minimo 03 e no maximo 100 caracteres")
	private String title;
	
	@NotBlank(message = "O atributo TEXTO é Obrigatorio!")
	@Size(min = 3, max = 100, message = "O atributo TEXTO deve conter no minimo 03 e no maximo 100 caracteres")
	private String text;
	
	@UpdateTimestamp
	private LocalDateTime date;

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public LocalDateTime getDate() {
		return date;
	}

	public void setDate(LocalDateTime date) {
		this.date = date;
	}
	
	
}