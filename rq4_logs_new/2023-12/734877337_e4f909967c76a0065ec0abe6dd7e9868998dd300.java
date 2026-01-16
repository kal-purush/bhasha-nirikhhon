package model.entities;

import model.enums.Color;

public abstract class AbstractShape implements Shape{
	// Atributos
	private Color color;
	
	// Métodos Getters e Setters
	public void setColor(Color color) {
		this.color = color;
	}
	
	public Color getColor() {
		return color;
	}
	
	// Método Construtor
	public AbstractShape(Color color) {
		this.color = color;
	}
}