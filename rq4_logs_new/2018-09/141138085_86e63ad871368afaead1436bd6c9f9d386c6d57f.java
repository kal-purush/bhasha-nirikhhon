package cms.sre.persistenceapi.controller;

import cms.sre.dna_common_data_model.product_list.Product;
import cms.sre.persistenceapi.service.ProductPersistenceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
public class ProductController {

    private ProductPersistenceService productPersistenceService;

    @Autowired
    public ProductController(ProductPersistenceService productPersistenceService){
        this.productPersistenceService = productPersistenceService;
    }


    @GetMapping("/products")
    public List<Product> getProducts(){ return this.productPersistenceService.getProducts(); }

    @GetMapping("/products/{title}")
    public List<Product> getProductsByTitle(@PathVariable("title") String title){ return this.productPersistenceService.getProductsByTitle(title); }

    @GetMapping("/productsByScmLocation/{scmLocation}")
    public List<Product> getProductsByScmLocation(@PathVariable("scmLocation") String scmLocation){ return this.productPersistenceService.getProductsByScmLocation(scmLocation); }

    @PutMapping("/products")
    @PostMapping("/products")
    public List<Product> upsertProducts(List<Product> products){
        List<Product> ret = null;
        if(this.productPersistenceService.upsert(products)){
            ret = products;
        }
        return ret;
    }

    @DeleteMapping("/products")
    public List<Product> deleteProducts(List<Product> products){
        return products;
    }
}