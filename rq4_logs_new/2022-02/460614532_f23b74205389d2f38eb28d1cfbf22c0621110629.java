package ouacheter.backend.controllers;

import org.springframework.web.bind.annotation.*;
import ouacheter.backend.entities.Asset;
import ouacheter.backend.services.AssetService;

import java.util.List;

@RestController
public class AssetController {


    private final AssetService AssetService;

    public AssetController(AssetService AssetService) {
        this.AssetService = AssetService;
    }


    @GetMapping("/Assets")
    List<Asset> all() {
        return AssetService.all();
    }
    @PostMapping("/Assets")
    Asset newAsset(@RequestBody Asset newAsset) {
        return AssetService.newAsset(newAsset);
    }

    // Single item

    @GetMapping("/Asset/{id}")
    Asset one(@PathVariable int id) {

        return AssetService.one(id);

    }

    @PutMapping("/Asset/{id}")
    Asset replaceAsset(@RequestBody Asset newAsset, @PathVariable int id) {

        return AssetService.replaceDescription(newAsset,id);

    }

    @DeleteMapping("/Asset/{id}")
    void deleteAsset(@PathVariable int id) {
        AssetService.deleteAsset(id);
    }
}