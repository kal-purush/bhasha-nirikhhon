package model.entities;

import model.enums.Color;

public class Circle extends Shape{
	// Atributos
	private Double radius;
	//private Double PI = 3.141516;
	
	// Método Construtor
	public Circle(Color color, Double radius) {
		super(color);
		this.radius = radius;
	}
	
	// Métodos Getters e Setters
	public void setRadius(Double radius) {
		this.radius = radius;
	}
	
	public Double getRadius() {
		return radius;
	}
	
	// Métodos
	@Override
	public double area() {
		return Math.PI * radius * radius;
	}
	
}