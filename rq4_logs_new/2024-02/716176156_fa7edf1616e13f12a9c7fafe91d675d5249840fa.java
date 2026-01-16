package mate.bookshopapp.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import java.util.List;
import lombok.RequiredArgsConstructor;
import mate.bookshopapp.dto.order.CreateOrderDto;
import mate.bookshopapp.dto.order.OrderResponseDto;
import mate.bookshopapp.dto.order.UpdateOrderStatusDto;
import mate.bookshopapp.dto.order.item.OrderItemResponseDto;
import mate.bookshopapp.model.User;
import mate.bookshopapp.service.OrderService;
import org.springframework.data.domain.Pageable;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Tag(name = "Order management", description = "Endpoints for maneging orders")
@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
public class OrderController {
    private final OrderService orderService;

    @PostMapping
    @PreAuthorize("hasRole('ROLE_USER')")
    @Operation(summary = "Create new order",
            description = "Enter your shipping address to create a new order")
    public OrderResponseDto createOrder(@RequestBody @Valid CreateOrderDto createOrderDto,
                                        Authentication authentication) {
        User user = (User) authentication.getPrincipal();
        return orderService.createOrder(createOrderDto, user.getId());
    }

    @GetMapping
    @PreAuthorize("hasRole('ROLE_USER')")
    @Operation(summary = "Get all orders by user id",
            description = "Get a list of all available orders")
    public List<OrderResponseDto> getAllOrdersByUser(Authentication authentication,
                                                     Pageable pageable) {
        User user = (User) authentication.getPrincipal();
        return orderService.findAllByUser(user.getId(), pageable);
    }

    @PatchMapping("/{orderId}")
    @PreAuthorize("hasRole('ROLE_ADMIN')")
    @Operation(summary = "Update order status", description = "Enter a new status to update")
    public OrderResponseDto updateOrderStatus(@RequestBody
                                                  @Valid UpdateOrderStatusDto updateOrderStatusDto,
                                              @PathVariable Long orderId) {
        return orderService.updateOrderStatus(updateOrderStatusDto, orderId);
    }

    @GetMapping("/{orderId}/items")
    @PreAuthorize("hasRole('ROLE_USER')")
    @Operation(summary = "Get all order items by order id",
            description = "Enter order id to get a list of all available order items")
    public List<OrderItemResponseDto> getAllByOrderId(@PathVariable Long orderId,
                                                      Pageable pageable) {
        return orderService.findAllByOrderId(orderId, pageable);
    }

    @GetMapping("/{orderId}/items/{itemId}")
    @PreAuthorize("hasRole('ROLE_USER')")
    @Operation(summary = "Retrieve a specific order item by order id and order item id",
            description = "Enter the order id and order item id to retrieve specific order item")
    public OrderItemResponseDto getOrderItemByOrderAndOrderItemIds(
            @PathVariable Long orderId,
            @PathVariable Long itemId) {
        return orderService.getOrderItemByOrderAndOrderItemIds(orderId, itemId);
    }
}