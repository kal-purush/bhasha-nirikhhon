package kz.bitlab.G118security.controller;

import kz.bitlab.G118security.model.Item;
import kz.bitlab.G118security.service.ItemService;
import lombok.RequiredArgsConstructor;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/items")
@RequiredArgsConstructor
public class ItemController {
    private final ItemService itemService;

    @GetMapping
    public List<Item> getItems() {
        return itemService.getItems();
    }

    @PostMapping
    public Item addItem(@RequestBody Item item) {
        return itemService.create(item);
    }

    @PutMapping
    public Item editItem(@RequestBody Item item) {
        return itemService.editItem(item);
    }

    @GetMapping("/{id}")
    public Item getItem(@PathVariable Long id){
        return itemService.getItemById(id);
    }

    @DeleteMapping("/{id}")
    public void deleteItem(@PathVariable Long id){
        itemService.deleteItemById(id);
    }

    @PatchMapping("/{id}")
    public Item editItem(@PathVariable Long id, @RequestParam Integer point){
        return itemService.editMark(id, point);
    }
}