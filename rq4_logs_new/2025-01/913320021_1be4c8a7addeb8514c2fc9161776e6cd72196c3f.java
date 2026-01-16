package com.management.restaurant.DTO.ordens;

import com.management.restaurant.DTO.client.ClientResponseDTO;
import com.management.restaurant.enums.StatusOrden;
import com.management.restaurant.models.client.Client;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;
import java.util.List;

@Getter
@Setter
public class OrdenResponseDTO {
  private Long id;
  private Double priceTotal;
  private LocalDateTime dateOrder;
  private StatusOrden statusOrder;
  private ClientResponseDTO client;
  private List<ItemResponseDTO> items;
}