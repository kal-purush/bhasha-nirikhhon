package com.management.restaurant.services;

import com.management.restaurant.DTO.ordens.*;
import com.management.restaurant.enums.StatusOrden;
import com.management.restaurant.factories.IOrdenFactory;
import com.management.restaurant.models.client.Client;
import com.management.restaurant.models.order.Item;
import com.management.restaurant.models.order.Orden;
import com.management.restaurant.models.restaurant.Dish;
import com.management.restaurant.repositories.ClientRepository;
import com.management.restaurant.repositories.DishRepository;
import com.management.restaurant.repositories.OrdenRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OrdenServiceTest {

    @Mock
    private OrdenRepository ordenRepository;
    @Mock
    private IOrdenFactory IOrdenFactory;
    @Mock
    private ClientRepository clientRepository;
    @Mock
    private DishRepository dishRepository;
    @Mock
    private DishService dishService;
    @Mock
    private ClientService clientService;

    @InjectMocks
    private OrdenService ordenService;
    private OrdenRequestDTO ordenRequestDTO;
    private Orden orden;
    private Client client;
    private Dish dish;
    private Item item;

    @BeforeEach
    void setUp() {
      MockitoAnnotations.openMocks(this);
      ordenRequestDTO = new OrdenRequestDTO();
      ordenRequestDTO.setClientId(1L);
      ordenRequestDTO.setItems(List.of(new ItemRequestDTO("Dish 1", 20.0, 2)));
      client = new Client();
      client.setId(1L);
      client.setIsFrecuent(true);

      dish = new Dish();
      dish.setName("Dish 1");
      dish.setPrice(20.0);

      item = new Item();
      item.setName("Dish 1");
      item.setQuantity(2);
      item.setDish(dish);
      item.setPrice(dish.getPrice());

      orden = new Orden();
      orden.setClient(client);
      orden.setItems(Collections.singletonList(item));
      orden.setPriceTotal(40.0);
    }

    @Test
    @DisplayName("Traer orden por id")
    void getOrdenById() {
      when(ordenRepository.findById(any(Long.class))).thenReturn(Optional.of(orden));
      OrdenResponseDTO response = ordenService.getOrdenById(1L);
      assertNotNull(response);
      assertEquals(40.0, response.getPriceTotal());

      verify(ordenRepository, times(1)).findById(1L);
    }
    @Test
    @DisplayName("Traer todas las ordenes")
    void getAllOrdenes() {
      when(ordenRepository.findAll()).thenReturn(Arrays.asList(orden));
      List<OrdenResponseDTO> responseList = ordenService.getAllOrdenes();
      assertNotNull(responseList);
      assertFalse(responseList.isEmpty());
      assertEquals(40.0, responseList.get(0).getPriceTotal());

      verify(ordenRepository, times(1)).findAll();
  }
  @Test
  @DisplayName("Caso negativo no devuelve las ordenes")
  void getAllOrdenes_Exception() {
      when(ordenRepository.findAll()).thenThrow(new RuntimeException("Mock Exception"));
       RuntimeException exception = assertThrows(RuntimeException.class, () -> {
         ordenService.getAllOrdenes();
       });
       assertEquals("Error al obtener todas las órdenes", exception.getMessage());
    }

  @Test
  @DisplayName("Eliminar todas las ordenes")
  void deleteOrden() {
    doNothing().when(ordenRepository).deleteById(any(Long.class));

    ordenService.deleteOrden(1L);

    verify(ordenRepository, times(1)).deleteById(1L);
  }
  @Test
  @DisplayName("Cambiar el estado de las ordenes")
  void changeStateOrder() {
    when(ordenRepository.findById(any(Long.class))).thenReturn(Optional.of(orden));
    when(ordenRepository.save(any(Orden.class))).thenReturn(orden);
    ordenService.changeStateOrder(1L, StatusOrden.COMPLETED);
    verify(ordenRepository, times(1)).findById(1L);
    verify(ordenRepository, times(1)).save(orden);
  }
  @Test
  void testChangeStateOrder_StatusNotRecognized() {
    when(ordenRepository.findById(any(Long.class))).thenReturn(Optional.of(orden));
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
      ordenService.changeStateOrder(1L, null);
    });
    assertTrue(exception.getMessage().contains("Estado no reconocido"));
    verify(ordenRepository, times(1)).findById(1L);
    }

  @Test
  @DisplayName("Encontrar plato por nombre")
  void findDishByName_Exist() {
      Dish mockDish = new Dish();
      mockDish.setName("Dish 1");
      mockDish.setPrice(20.0);
      when(dishService.findDishByName("Dish 1")).thenReturn(mockDish);
      Dish dish = ordenService.findDishByName("Dish 1");
      assertNotNull(dish); assertEquals("Dish 1", dish.getName());
      verify(dishService, times(1)).findDishByName("Dish 1");
    }
  @Test
  @DisplayName("Caso negativo, no encuentra el plato")
  void findDishByNameThrowsException() {
      when(dishService.findDishByName(any(String.class))).thenReturn(null);
      RuntimeException exception = assertThrows(RuntimeException.class, () -> {
        ordenService.findDishByName("Dish 1"); });
      assertEquals("El plato con nombre Dish 1 no existe", exception.getMessage());

      verify(dishService, times(1)).findDishByName("Dish 1");
    }

  @Test
  @DisplayName("Nofificacion de los observadores de platos")
  void notifyDishObserversForItems() {
      Dish mockDish = new Dish();
      mockDish.setName("Dish 1");
      mockDish.setPrice(20.0);

      Item item1 = new Item();
      item1.setName("Item 1");
      item1.setDish(mockDish);

      Item item2 = new Item();
      item2.setName("Item 2");
      item2.setDish(null);

      List<Item> items = Arrays.asList(item1, item2);
      doNothing().when(dishService).notifyDishObservers(any(Dish.class));
      ordenService.notifyDishObserversForItems(items);
      verify(dishService, times(1)).notifyDishObservers(mockDish);
    }
  @Test
  @DisplayName("Encuentra cliente por id")
  void findClientByIdFound() {
      Client mockClient = new Client();
      mockClient.setId(1L);
      when(clientRepository.findById(any(Long.class))).thenReturn(Optional.of(mockClient));
      Client client = ordenService.findClientById(1L);

      assertNotNull(client);
      assertEquals(1L, client.getId());
      verify(clientRepository, times(1)).findById(1L);
    }

  @Test
  @DisplayName("No encuentra cliente por id")
  void findClientByIdNotFound() {
     when(clientRepository.findById(any(Long.class))).thenReturn(Optional.empty());
     RuntimeException exception = assertThrows(RuntimeException.class, () -> {
       ordenService.findClientById(1L); });
     assertEquals("Cliente no encontrado", exception.getMessage());
     verify(clientRepository, times(1)).findById(1L);
    }
  @Test
  @DisplayName("Contiene a item orden y plato")
  void setItemOrdenAndDish() {
      Orden orden = new Orden();
      Dish mockDish = new Dish();
      mockDish.setName("Dish 1");
      mockDish.setPrice(20.0);

      Item item = new Item();
      item.setName("Dish 1");

      when(dishService.findDishByName(any(String.class))).thenReturn(mockDish);
      ordenService.setItemOrdenAndDish(item, orden);

      assertEquals(orden, item.getOrden());
      assertEquals(mockDish, item.getDish());
      verify(dishService, times(1)).findDishByName("Dish 1");
    }
  @Test
  @DisplayName("Calcula el precio total")
  void calculateTotalPrice() {
      Item item1 = new Item();
      item1.setPrice(10.0);
      item1.setQuantity(2);

      Item item2 = new Item();
      item2.setPrice(20.0);
      item2.setQuantity(1);
      List<Item> items = Arrays.asList(item1, item2);
      Double totalPrice = ordenService.calculateTotalPrice(items);
      assertEquals(40.0, totalPrice);
    }
  @Test
  @DisplayName("Aplica descuento")
  void applyDiscount() {
      Double priceTotal = 100.0;
      Double discountPercentage = 20.0;
      Double discountedPrice = ordenService.applyDiscount(priceTotal, discountPercentage);
      assertEquals(80.0, discountedPrice);
    }
  @Test
  @DisplayName("valida items")
  void convertAndValidateItem() {
    ItemRequestDTO itemRequestDTO = new ItemRequestDTO("Dish 1", 20.0, 2);
    when(dishService.findDishByName(any(String.class))).thenReturn(dish);
    Item item = ordenService.convertAndValidateItem(itemRequestDTO);
    assertEquals(dish, item.getDish());
    verify(dishService, times(1)).findDishByName("Dish 1");
    }

  @Test
  @DisplayName("Valida que la lista de item no este vacia")
  void validateAndConvertItemsNullOrEmpty() {
      IllegalArgumentException nullException = assertThrows(IllegalArgumentException.class, () -> {
        ordenService.validateAndConvertItems(null);
      });
      assertEquals("El pedido debe tener al menos un item.", nullException.getMessage());
      IllegalArgumentException emptyException = assertThrows(IllegalArgumentException.class, () -> {
        ordenService.validateAndConvertItems(Collections.emptyList()); });
      assertEquals("El pedido debe tener al menos un item.", emptyException.getMessage());
    }
  @Test
  @DisplayName("Valida que la lista de item")
  void validateAndConvertItemsValidList() {
      ItemRequestDTO itemRequestDTO = new ItemRequestDTO("Dish 1", 20.0, 2);
      List<ItemRequestDTO> itemRequestDTOList = Collections.singletonList(itemRequestDTO);
      when(dishService.findDishByName(any(String.class))).thenReturn(dish);
      List<Item> items = ordenService.validateAndConvertItems(itemRequestDTOList);
      assertNotNull(items);
      assertEquals(1, items.size());
      Item item = items.get(0); assertEquals(dish, item.getDish());
      verify(dishService, times(1)).findDishByName("Dish 1");
    }
  @Test
  @DisplayName("Actualizar orden")
  void testUpdateOrdenSuccess() {
    Long ordenId = 1L;
    when(ordenRepository.findById(ordenId)).thenReturn(Optional.of(orden));
    when(clientRepository.findById(ordenRequestDTO.getClientId())).thenReturn(Optional.of(client));
    when(dishService.findDishByName("Dish 1")).thenReturn(dish);
    List<Item> items = Collections.singletonList(item);
    orden.setItems(items);
    orden.setPriceTotal(40.0);
    when(ordenRepository.save(any(Orden.class))).thenReturn(orden);
    OrdenResponseDTO response = ordenService.updateOrden(ordenId, ordenRequestDTO);
    assertNotNull(response);
    assertEquals(40.0, response.getPriceTotal());
    verify(ordenRepository, times(1)).findById(ordenId);
    verify(clientRepository, times(1)).findById(ordenRequestDTO.getClientId());
    verify(dishService, times(1)).findDishByName("Dish 1");
    verify(ordenRepository, times(1)).save(orden);
    }


}