package com.sabina.member.serv.model;

import java.time.LocalDate;

import lombok.Data;

@Data
public class Location {
	private String name;
	private LocalDate deliveryDate;
	private String owner;
	private ShoppingListItem item;
}