package ouacheter.backend.controllers;

import org.springframework.web.bind.annotation.*;
import ouacheter.backend.entities.Shop;
import ouacheter.backend.services.ShopService;

import java.util.List;

@RestController
public class ShopController {


    private final ShopService ShopService;

    public ShopController(ShopService ShopService) {
        this.ShopService = ShopService;
    }


    @GetMapping("/Shops")
    List<Shop> all() {
        return ShopService.all();
    }
    @PostMapping("/Shops")
    Shop newShop(@RequestBody Shop newShop) {
        return ShopService.newShop(newShop);
    }

    // Single item

    @GetMapping("/Shop/{id}")
    Shop one(@PathVariable int id) {

        return ShopService.one(id);

    }

    @PutMapping("/Shop/{id}")
    Shop replaceShop(@RequestBody Shop newShop, @PathVariable int id) {

        return ShopService.replaceDescription(newShop,id);

    }

    @DeleteMapping("/Shop/{id}")
    void deleteShop(@PathVariable int id) {
        ShopService.deleteShop(id);
    }
}