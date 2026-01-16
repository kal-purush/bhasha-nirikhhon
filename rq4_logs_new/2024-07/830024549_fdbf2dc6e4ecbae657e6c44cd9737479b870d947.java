package com.example.vicenteytech.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ItemRequestDTO {

	@JsonProperty("quantity")
	private Integer quantity;
	
	@JsonProperty("item")
	private ItemDTO item;

}