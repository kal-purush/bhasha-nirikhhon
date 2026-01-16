package net.sys.loja.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;

@Entity(name="carrinho")
public class Carrinho {

	@Id
	@Column
	private Long id;
	
	private Pedido pedido;
	
	 
	 
 
	
	
}