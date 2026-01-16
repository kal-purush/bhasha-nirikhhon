//package jm.stockx.controller.rest.user;
//
//import io.swagger.v3.oas.annotations.Operation;
//import io.swagger.v3.oas.annotations.media.Content;
//import io.swagger.v3.oas.annotations.media.Schema;
//import io.swagger.v3.oas.annotations.responses.ApiResponse;
//import io.swagger.v3.oas.annotations.tags.Tag;
//import jm.stockx.ItemService;
//import jm.stockx.dto.ItemDto;
//import jm.stockx.dto.PageDto;
//import jm.stockx.entity.Item;
//import jm.stockx.util.Response;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.http.HttpStatus;
//import org.springframework.web.bind.annotation.*;
//
//import java.util.List;
//
//@RestController
//@RequestMapping(value = "/rest/api/items")
//@Tag(name = "item", description = "Item API")
//public class ItemRestController {
//    private static final Logger logger = LoggerFactory.getLogger(ItemRestController.class);
//
//    private final ItemService itemService;
//
//    @Autowired
//    public ItemRestController(ItemService itemService) {
//        this.itemService = itemService;
//    }
//
//    @GetMapping
//    @Operation(
//            operationId = "getAllItems",
//            summary = "Get all items",
//            responses = {
//                    @ApiResponse(responseCode = "200",
//                            content = @Content(
//                                    mediaType = "application/json",
//                                    schema = @Schema(type = "array", implementation = Item.class)
//                            ),
//                            description = "OK: got items list"
//                    ),
//                    @ApiResponse(responseCode = "400", description = "NOT_FOUND: no items found")
//            })
//    public Response<List<Item>> getAllItems() {
//        List<Item> items = itemService.getAll();
//        logger.info("Получен список товаров. Всего {} записей.", items.size());
//        return Response.ok(items);
//    }
//
//    @GetMapping(value = "/{id}")
//    @Operation(
//            operationId = "getItemById",
//            summary = "Get item by id",
//            responses = {
//                    @ApiResponse(responseCode = "200",
//                            content = @Content(
//                                    mediaType = "application/json",
//                                    schema = @Schema(type = "array", implementation = Item.class)
//                            ),
//                            description = "OK: got item"
//                    ),
//                    @ApiResponse(responseCode = "400", description = "NOT_FOUND: no items with this item id")
//            })
//    public Response<Item> getItemById(@PathVariable("id") Long id) {
//        Item item = itemService.get(id);
//        if (item == null) {
//            logger.warn("Товар с id = {} в базе не найден", id);
//            return Response.error(HttpStatus.BAD_REQUEST, "Item not found");
//        }
//        logger.info("Получен товар {} ", item);
//        return Response.ok(item);
//    }
//
//    @PostMapping
//    @Operation(
//            operationId = "createItem",
//            summary = "Create item",
//            responses = {
//                    @ApiResponse(responseCode = "200",
//                            content = @Content(
//                                    mediaType = "application/json",
//                                    schema = @Schema(implementation = Item.class)
//                            ),
//                            description = "OK: item created"
//                    ),
//                    @ApiResponse(responseCode = "400", description = "NOT_FOUND: item was not created")
//            })
//    public Response<?> createItem(Item item) {
//        String itemName = item.getName();
//        if (itemService.getItemByName(itemName) != null) {
//            logger.warn("Товар {} уже существует в базе", itemName);
//            return Response.error(HttpStatus.BAD_REQUEST,"This item already exists in database");
//        }
//        itemService.create(item);
//        logger.info("Товар {} успешно создан", itemName);
//        return Response.ok().build();
//    }
//
//    @PutMapping
//    @Operation(
//            operationId = "updateItem",
//            summary = "Update item",
//            responses = {
//                    @ApiResponse(responseCode = "200",
//                            content = @Content(
//                                    mediaType = "application/json",
//                                    schema = @Schema(implementation = Item.class)
//                            ),
//                            description = "OK: item updated successfully"
//                    ),
//                    @ApiResponse(responseCode = "400", description = "NOT_FOUND: unable to update item")
//            })
//    public Response<?> updateItem(Item item) {
//        String itemName = item.getName();
//        if (itemService.getItemByName(itemName) == null) {
//            logger.warn("Товар {} в базе не найден", itemName);
//            return Response.error(HttpStatus.BAD_REQUEST,"Item not found");
//        }
//        itemService.update(item);
//        logger.info("Товар {} успешно обновлен", itemName);
//        return Response.ok().build();
//    }
//
//    @DeleteMapping(value = "/{id}")
//    @Operation(
//            operationId = "deleteItem",
//            summary = "Delete item",
//            responses = {
//                    @ApiResponse(responseCode = "200", description = "OK: item deleted successfully"),
//                    @ApiResponse(responseCode = "400", description = "NOT FOUND: no item with such id")
//            }
//    )
//    public Response<?> deleteItem(@PathVariable("id") Long id) {
//        if (itemService.get(id) == null) {
//            logger.warn("Товар с id = {} в базе не найден", id);
//            return Response.error(HttpStatus.BAD_REQUEST,"Item not found");
//        }
//        itemService.delete(id);
//        logger.info("Товар с id = {} успешно удален", id);
//        return Response.ok().build();
//    }
//
//    @GetMapping("/itemInfo")
//    public Response<PageDto<ItemDto>> getItemInformation(@RequestParam(name = "page") Integer page,
//                                                         @RequestParam(name = "search") String search,
//                                                         @RequestParam(required = false, defaultValue = "15") Integer size) {
//        PageDto<ItemDto> pageDto = itemService.getPageOfItems(page, search, size);
//        return Response.ok(pageDto);
//    }
//}