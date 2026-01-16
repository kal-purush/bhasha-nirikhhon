package mate.academy.intro.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import java.util.List;
import lombok.RequiredArgsConstructor;
import mate.academy.intro.dto.order.CreateOrderRequestDto;
import mate.academy.intro.dto.order.OrderResponseDto;
import mate.academy.intro.dto.order.UpdateOrderRequestDto;
import mate.academy.intro.dto.orderitem.OrderItemResponseDto;
import mate.academy.intro.model.User;
import mate.academy.intro.service.order.OrderService;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Tag(name = "Order management", description = "Endpoints for managing orders")
@RequiredArgsConstructor
@RestController
@RequestMapping("/orders")
public class OrderController {
    private final OrderService orderService;

    @Operation(summary = "Create an order", description = "Create an order")
    @PostMapping
    @PreAuthorize("hasRole('USER')")
    CreateOrderRequestDto createOrder(Authentication authentication) {
        User user = (User) authentication.getPrincipal();
        return orderService.createOrder(user);
    }

    @GetMapping
    @PreAuthorize("hasRole('USER')")
    List<OrderResponseDto> getOrdersByUserId(Authentication authentication) {
        User user = (User) authentication.getPrincipal();
        return orderService.getOrders(user.getId());
    }

    @GetMapping("/{orderId}/items")
    @PreAuthorize("hasRole('USER')")
    List<OrderItemResponseDto> getOrderItemsByOrderId(
            Authentication authentication,
            @PathVariable Long orderId) {
        User user = (User) authentication.getPrincipal();
        return orderService.getOrderItemsByOrderId(user.getId(), orderId);
    }

    @GetMapping("/{orderId}/items/{itemId}")
    @PreAuthorize("hasRole('USER')")
    OrderItemResponseDto getOrderItem(
            Authentication authentication,
            @PathVariable Long orderId,
            @PathVariable Long itemId) {
        User user = (User) authentication.getPrincipal();
        return orderService.getOrderItemByOrderIdAndItemId(user.getId(), orderId, itemId);
    }

    @Operation(summary = "Update order status", description = "Update order status")
    @PatchMapping("/{id}")
    @PreAuthorize("hasRole('ADMIN')")
    UpdateOrderRequestDto updateOrderStatus(
            @PathVariable Long id,
            @RequestBody @Valid UpdateOrderRequestDto orderDto
    ) {
        return orderService.updateOrderStatus(id, orderDto);
    }
}