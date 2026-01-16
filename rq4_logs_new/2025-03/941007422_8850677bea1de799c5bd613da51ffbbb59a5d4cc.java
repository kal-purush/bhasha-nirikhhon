package com.example.auction.Product;

import com.example.auction.Product.Dto.ProductRequestDto;
import com.example.auction.Product.Dto.ProductResponseDto;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ProductServiceTest {

    @Mock
    private ProductRepository productRepository;

    @InjectMocks
    private ProductService productService;

    private Product product;
    private ProductRequestDto productRequestDto;

    @BeforeEach
    void setUp(){
        productRequestDto = new ProductRequestDto("name","content","imagePath");
        product = new Product(1L,productRequestDto);
    }

    @Test
    void addProduct() {
        when(productRepository.save(any(Product.class))).thenReturn(product);

        ProductResponseDto responseDto = productService.addProduct(1L,productRequestDto);

        assertThat(responseDto.getName()).isEqualTo(product.getName());
        verify(productRepository , times(1)).save(any(Product.class));
    }

    @Test
    void getProduct() {
        when(productRepository.findByIdOrElseThrow(anyLong())).thenReturn(product);

        ProductResponseDto responseDto =  productService.getProduct(1L);

        assertThat(responseDto).isNotNull();
        assertThat(responseDto.getName()).isEqualTo(product.getName());
        verify(productRepository, times(1)).findByIdOrElseThrow(1L);
    }

    @Test
    void updateProduct() {
        ProductRequestDto updateProductDto = new ProductRequestDto("name2","content2","imagePath2");
        when(productRepository.findByIdOrElseThrow(anyLong())).thenReturn(product);

        Product updateProduct = new Product(1L,updateProductDto);
        when(productRepository.save(any(Product.class))).thenReturn(updateProduct);

        ProductResponseDto responseDto = productService.updateProduct(1L,updateProductDto);
        assertThat(responseDto).isNotNull();
        assertThat(responseDto.getName()).isEqualTo(updateProduct.getName());
        verify(productRepository,times(1)).save(any(Product.class));
    }

    @Test
    void deleteProduct() {
        when(productRepository.findByIdOrElseThrow(anyLong())).thenReturn(product);

        ProductResponseDto responseDto = productService.deleteProduct(1L);

        assertThat(responseDto).isNotNull();
        assertThat(responseDto.getStatus()).isEqualTo(ProductStatus.DELETED);
        verify(productRepository,times(1)).save(any(Product.class));
    }
}