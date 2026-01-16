package book.store.intro.controller;

import book.store.intro.dto.order.request.CreateOrderRequestDto;
import book.store.intro.dto.order.request.UpdateOrderItemDto;
import book.store.intro.dto.order.response.OrderItemResponseDto;
import book.store.intro.dto.order.response.OrderResponseDto;
import book.store.intro.service.order.OrderItemService;
import book.store.intro.service.order.OrderService;
import jakarta.validation.Valid;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/orders")
public class OrderController {
    private final OrderService orderService;
    private final OrderItemService orderItemService;

    @GetMapping
    public List<OrderResponseDto> getAll() {
        return orderService.getAllByUser();
    }

    @PostMapping
    public OrderResponseDto create(@RequestBody @Valid CreateOrderRequestDto requestDto) {
        return orderService.createOrder(requestDto);
    }

    @PatchMapping("/{id}")
    public OrderResponseDto updateStatus(@PathVariable Long id,
                                         @RequestBody UpdateOrderItemDto itemDto) {
        return orderService.updateStatus(id, itemDto);
    }

    @GetMapping("/{id}/items")
    public List<OrderItemResponseDto> getAllItemsFromOrder(@PathVariable Long id) {
        return orderItemService.getAllFromOrder(id);
    }

    @GetMapping("/{orderId}/items/{id}")
    public OrderItemResponseDto getOrderItem(@PathVariable Long orderId,
                                         @PathVariable Long id) {
        return orderItemService.getFromOrderById(orderId, id);
    }

}