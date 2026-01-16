package br.com.cabalprivate.bean;

import java.util.Date;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;

@Entity
@SequenceGenerator(name = "seq_aluno", sequenceName = "aluno_seq", initialValue = 1, allocationSize = 1)
public class Aluno {
	
	@Id
	@GeneratedValue(strategy = 
	GenerationType.SEQUENCE, generator = "seq_aluno")
	private Integer id;
	private String nome;
	private String matricula;
	
	

	
	public String getNome() {
		return nome;
	}
	public void setNome(String nome) {
		this.nome = nome;
	}
	public String getMatricula() {
		return matricula;
	}
	public void setMatricula(String matricula) {
		this.matricula = matricula;
	}
	
}