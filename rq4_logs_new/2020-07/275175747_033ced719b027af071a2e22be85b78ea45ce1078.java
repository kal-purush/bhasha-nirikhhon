package jm.stockx.controller.rest.user;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jm.stockx.ItemService;
import jm.stockx.entity.Item;
import jm.stockx.util.Response;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping(value = "/rest/api/user/items")
@Tag(name = "item", description = "Item API")
@Slf4j
public class UserItemRestController {
    private final ItemService itemService;

    @Autowired
    public UserItemRestController(ItemService itemService) {
        this.itemService = itemService;
    }

    @GetMapping
    @Operation(
            operationId = "getAllItems",
            summary = "Get all items",
            responses = {
                    @ApiResponse(responseCode = "200",
                            content = @Content(
                                    mediaType = "application/json",
                                    schema = @Schema(type = "array", implementation = Item.class)
                            ),
                            description = "OK: got items list"
                    ),
                    @ApiResponse(responseCode = "400", description = "BAD_REQUEST: no items found")
            })
    public Response<List<Item>> getAllItems() {
        List<Item> items = itemService.getAll();
        log.info("Получен список товаров. Всего {} записей.", items.size());
        return Response.ok(items);
    }

    @GetMapping(value = "/{id}")
    @Operation(
            operationId = "getItemById",
            summary = "Get item by id",
            responses = {
                    @ApiResponse(responseCode = "200",
                            content = @Content(
                                    mediaType = "application/json",
                                    schema = @Schema(type = "array", implementation = Item.class)
                            ),
                            description = "OK: got item"
                    ),
                    @ApiResponse(responseCode = "400", description = "BAD_REQUEST: no items with this item id")
            })
    public Response<Item> getItemById(@PathVariable("id") Long id) {
        Item item = itemService.get(id);
        if (item == null) {
            log.warn("Товар с id = {} в базе не найден", id);
            return Response.error(HttpStatus.BAD_REQUEST, "Item not found");
        }
        log.info("Получен товар {} ", item);
        return Response.ok(item);
    }
}