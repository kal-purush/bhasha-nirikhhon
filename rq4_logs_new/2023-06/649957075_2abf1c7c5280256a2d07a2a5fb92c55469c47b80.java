package ar.com.lokywolf2295.proyectocarritodecompras.controller;

import ar.com.lokywolf2295.proyectocarritodecompras.model.dto.request.*;
import ar.com.lokywolf2295.proyectocarritodecompras.model.dto.response.ProductResponseDTO;
import ar.com.lokywolf2295.proyectocarritodecompras.service.IProductService;

import io.swagger.annotations.ApiOperation;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import static org.springframework.http.HttpStatus.*;

@RestController
@RequestMapping ("/product")
public class ProductController {
    
    @Autowired
    private IProductService productService;

    @PostMapping("/new")
    @ApiOperation("Creación de un producto")
    public ResponseEntity<ProductResponseDTO> createProduct (@RequestBody ProductRequestDTO productRequestDTO){
        ProductResponseDTO productResponseDTO = productService.createProduct(productRequestDTO);
        return  ResponseEntity.status(CREATED).body(productResponseDTO);
    }
    
    @PutMapping("/update/{id}")
    @ApiOperation("Actualizar información de un Producto existente, por su ID")
    public ResponseEntity<ProductResponseDTO> updateProduct (@PathVariable("id") Long id, @RequestBody ProductRequestUpdateDTO productRequestUpdateDTO ){
        ProductResponseDTO productResponseDTO = productService.updateProduct(id, productRequestUpdateDTO);
        return ResponseEntity.status(OK).body(productResponseDTO);
    }
    
    @DeleteMapping ("/delete/{id}")
    @ApiOperation("Eliminar el producto de la BD")
    public ResponseEntity<Void> deleteProduct (@PathVariable ("id") Long id){
        productService.deleteProduct(id);
        return ResponseEntity.status(NO_CONTENT).build();
    } 

    @GetMapping("/details/{id}")
    public ResponseEntity<ProductResponseDTO> getProductDetails(@PathVariable Long id) {
        ProductResponseDTO productResponseDTO = productService.getProductDetails(id);
        return ResponseEntity.status(OK).body(productResponseDTO);
    }

    @GetMapping("/listProducts")
    @ResponseBody
    @ApiOperation("Listado de productos en la base de datos")
    public ResponseEntity<Page<ProductResponseDTO>> getAllProducts(@RequestParam(name = "page") int page){
        return ResponseEntity.status(OK).body(productService.getAllProducts(page));
    }
}