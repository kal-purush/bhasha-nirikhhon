package com.management.restaurant.controllers;

import com.management.restaurant.DTO.client.ClientResponseDTO;
import com.management.restaurant.DTO.ordens.DishDTO;
import com.management.restaurant.DTO.ordens.ItemRequestDTO;
import com.management.restaurant.DTO.ordens.ItemResponseDTO;
import com.management.restaurant.DTO.ordens.OrdenRequestDTO;
import com.management.restaurant.DTO.ordens.OrdenResponseDTO;
import com.management.restaurant.DTO.restaurant.DishRequestDTO;
import com.management.restaurant.enums.StatusOrden;
import com.management.restaurant.models.order.Item;
import com.management.restaurant.models.restaurant.Dish;
import com.management.restaurant.services.OrdenService;
import com.management.restaurant.services.observer.ObserverManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.reactive.function.BodyInserters;

import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OrdenControllerTest {
  private WebTestClient webTestClient;
  private OrdenService ordenService;;

  @BeforeEach
  void setUp() {
    ordenService = mock(OrdenService.class);
    webTestClient = WebTestClient.bindToController(new OrdenController(ordenService)).build();
  }

  private OrdenRequestDTO createOrdenRequestDTO(){
    OrdenRequestDTO ordenRequestDTO = new OrdenRequestDTO();
    ordenRequestDTO.setPriceTotal(0.00);
    ordenRequestDTO.setClientId(7L);
    ordenRequestDTO.setStatusOrder(StatusOrden.PENDING);

    ItemRequestDTO itemRequestDTO = new ItemRequestDTO();
    itemRequestDTO.setName("Mute");
    itemRequestDTO.setPrice(8.0);
    itemRequestDTO.setQuantity(1);

    ordenRequestDTO.setItems(List.of(itemRequestDTO));
    return ordenRequestDTO;
  }
  private OrdenResponseDTO createOrdenResponseDTO(){
    OrdenResponseDTO ordenResponseDTO = new OrdenResponseDTO();
    ordenResponseDTO.setId(1L);
    ordenResponseDTO.setPriceTotal(0.00);
    ordenResponseDTO.setStatusOrder(StatusOrden.PENDING);

    ClientResponseDTO clientResponseDTO = new ClientResponseDTO();
    clientResponseDTO.setId(7L);
    clientResponseDTO.setName("Elena");
    clientResponseDTO.setEmail("elena@gmail.com");
    clientResponseDTO.setNumberPhone("3471112233");
    clientResponseDTO.setIsFrecuent(false);

    ordenResponseDTO.setClient(clientResponseDTO);
    ordenResponseDTO.setIsFrecuent(false);
    ordenResponseDTO.setDateOrder(LocalDateTime.of(2025, 1, 7, 9, 35, 53, 3438712));

    ItemResponseDTO itemResponseDTO = new ItemResponseDTO();
    itemResponseDTO.setId(1L);
    itemResponseDTO.setName("Mute");
    itemResponseDTO.setQuantity(1);

    DishDTO dish = new DishDTO();
    dish.setId(8L);
    dish.setName("Mute");
    dish.setPrice(8.0);
    dish.setPopular(false);

    itemResponseDTO.setDish(dish);

    ordenResponseDTO.setItems(List.of(itemResponseDTO));
    return ordenResponseDTO;
  }

  private List<OrdenResponseDTO> createOrdenResponseDTOList() {
    return List.of(createOrdenResponseDTO());
  }
  private void assertOrdenResponseDTO(OrdenResponseDTO expected, OrdenResponseDTO actual) {
    assertEquals(expected.getId(), actual.getId());
    assertEquals(expected.getPriceTotal(), actual.getPriceTotal());
    assertEquals(expected.getStatusOrder(), actual.getStatusOrder());
    assertEquals(expected.getClient().getId(), actual.getClient().getId());
    assertEquals(expected.getClient().getName(), actual.getClient().getName());
    assertEquals(expected.getClient().getEmail(), actual.getClient().getEmail());
    assertEquals(expected.getClient().getNumberPhone(), actual.getClient().getNumberPhone());
    assertEquals(expected.getClient().getIsFrecuent(), actual.getClient().getIsFrecuent());
    assertEquals(expected.getItems().size(), actual.getItems().size());
    for (int i = 0; i < expected.getItems().size(); i++) {
      assertItemResponseDTO(expected.getItems().get(i), actual.getItems().get(i));
    }
  }
  private void assertItemResponseDTO(ItemResponseDTO expected, ItemResponseDTO actual) {
    assertEquals(expected.getId(), actual.getId());
    assertEquals(expected.getName(), actual.getName());
    assertEquals(expected.getQuantity(), actual.getQuantity());
    assertEquals(expected.getDish().getId(), actual.getDish().getId());
    assertEquals(expected.getDish().getName(), actual.getDish().getName());
    assertEquals(expected.getDish().getPrice(), actual.getDish().getPrice());
    assertEquals(expected.getDish().getPopular(), actual.getDish().getPopular());
  }

  @Test
  @DisplayName("Crear orden")
  void createOrden() {
   OrdenRequestDTO ordenRequestDTO = createOrdenRequestDTO();
   OrdenResponseDTO ordenResponseDTO = createOrdenResponseDTO();
   when(ordenService.createOrden(any(OrdenRequestDTO.class))).thenReturn(ordenResponseDTO);

    webTestClient.post()
      .uri("/api/ordenes")
      .bodyValue(ordenRequestDTO)
      .exchange()
      .expectStatus().isOk()
      .expectHeader().contentType(MediaType.APPLICATION_JSON)
      .expectBody(OrdenResponseDTO.class)
      .value(response-> assertOrdenResponseDTO(ordenResponseDTO, response));

    verify(ordenService).createOrden(any(OrdenRequestDTO.class));
  }

  @Test
  @DisplayName("Traer todos los pedidos")
  void getAllOrdenes() {

    List<OrdenResponseDTO> ordenRequestDTOList = createOrdenResponseDTOList();
    when(ordenService.getAllOrdenes()).thenReturn(ordenRequestDTOList);

      webTestClient.get()
        .uri("/api/ordenes")
        .exchange()
        .expectStatus().isOk()
        .expectHeader().contentType(MediaType.APPLICATION_JSON)
        .expectBodyList(OrdenResponseDTO.class)
        .value(response -> {
           assertEquals(ordenRequestDTOList.size(), response.size());
          for (int i = 0; i < ordenRequestDTOList.size(); i++) {
            assertOrdenResponseDTO(ordenRequestDTOList.get(i), response.get(i));
          }
        });
      verify(ordenService).getAllOrdenes();
  }

  @Test
  @DisplayName("Traer por id del pedido")
  void getOrdenById() {
    OrdenResponseDTO ordenResponseDTO = createOrdenResponseDTO();
    when(ordenService.getOrdenById(anyLong())).thenReturn(ordenResponseDTO);
    webTestClient.get() .uri("/api/ordenes/{id}", 1L)
      .exchange() .expectStatus().isOk()
      .expectHeader().contentType(MediaType.APPLICATION_JSON)
      .expectBody(OrdenResponseDTO.class)
      .value(response -> assertOrdenResponseDTO(ordenResponseDTO, response));
    verify(ordenService).getOrdenById(anyLong());
    }

  @Test
  @DisplayName("Actualizar el pedido")
  void updateOrden() {
    OrdenRequestDTO ordenRequestDTO = createOrdenRequestDTO();
    OrdenResponseDTO ordenResponseDTO = createOrdenResponseDTO();
    when(ordenService.updateOrden(anyLong(), any(OrdenRequestDTO.class))).thenReturn(ordenResponseDTO);
    webTestClient.put() .uri("/api/ordenes/{id}", 1L) .contentType(MediaType.APPLICATION_JSON)
      .body(BodyInserters.fromValue(ordenRequestDTO))
      .exchange()
      .expectStatus().isOk()
      .expectHeader().contentType(MediaType.APPLICATION_JSON)
      .expectBody(OrdenResponseDTO.class)
      .value(response -> assertOrdenResponseDTO(ordenResponseDTO, response));
    verify(ordenService).updateOrden(anyLong(), any(OrdenRequestDTO.class));
  }

  @Test
  @DisplayName("Eliminar el pedido")
  void deleteOrden() {
    webTestClient.delete()
      .uri("/api/ordenes/{id}", 1L)
      .exchange()
      .expectStatus().isNoContent();
    verify(ordenService).deleteOrden(anyLong());
  }

  @Test
  @DisplayName("Cambiar estado del pedido")
  void changeStateOrder() {
    OrdenResponseDTO ordenResponseDTO = createOrdenResponseDTO();
    ordenResponseDTO.setStatusOrder(StatusOrden.COMPLETED);

    ordenResponseDTO.setStatusOrder(StatusOrden.COMPLETED);
    doAnswer(invocation -> {
      Long id = invocation.getArgument(0);
      StatusOrden newStatus = invocation.getArgument(1);
      return null;
    }).when(ordenService).changeStateOrder(anyLong(), any(StatusOrden.class));

    when(ordenService.getOrdenById(anyLong())).thenReturn(ordenResponseDTO);

    webTestClient.put()
      .uri("/api/ordenes/{id}/{newStatus}", 1L, "COMPLETED")
      .exchange()
      .expectStatus().isOk()
      .expectHeader().contentType(MediaType.APPLICATION_JSON)
      .expectBody(OrdenResponseDTO.class)
      .value(response -> assertOrdenResponseDTO(ordenResponseDTO, response));

     verify(ordenService).changeStateOrder(anyLong(), any(StatusOrden.class));
     verify(ordenService).getOrdenById(anyLong());
  }


}