package ar.com.lokywolf2295.proyectocarritodecompras.service;

import ar.com.lokywolf2295.proyectocarritodecompras.model.dto.request.ProductRequestDTO;
import ar.com.lokywolf2295.proyectocarritodecompras.model.dto.request.ProductRequestUpdateDTO;
import ar.com.lokywolf2295.proyectocarritodecompras.model.dto.response.ProductResponseDTO;
import ar.com.lokywolf2295.proyectocarritodecompras.model.entity.Product;
import org.springframework.data.domain.Page;

public interface IProductService {

    public ProductResponseDTO createProduct(ProductRequestDTO productRequestDto);

    public ProductResponseDTO updateProduct(Long id, ProductRequestUpdateDTO productRequestUpdateDTO);

    public void deleteProduct(Long id);

    Product getProductById(Long id);

    public ProductResponseDTO getProductDetails(Long id);

    public Page<ProductResponseDTO> getAllProducts(int page);
}