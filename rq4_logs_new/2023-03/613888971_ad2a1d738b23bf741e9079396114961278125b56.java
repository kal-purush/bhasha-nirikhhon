package com.casa.codigo.dto;

import com.casa.codigo.constants.Status;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ShopStatusDto {
  private String identifier;
  private Status status;
  
}