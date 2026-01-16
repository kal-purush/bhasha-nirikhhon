package com.h33kz.WarehouseManagementSystem.exceptions;

public class OrderNotFoundException extends RuntimeException{
  public OrderNotFoundException(String message){
    super(message);
  }
  
}