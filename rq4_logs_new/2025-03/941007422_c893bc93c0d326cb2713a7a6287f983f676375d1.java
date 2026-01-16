package com.example.auction.Product;

import com.example.auction.Auth.UserDetailsImpl;
import com.example.auction.Global.CommonResponseBody;
import com.example.auction.Product.Dto.ProductRequestDto;
import com.example.auction.Product.Dto.ProductResponseDto;
import com.example.auction.User.entity.Role;
import com.example.auction.User.entity.User;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ProductControllerTest {

    @InjectMocks
    private ProductController productController;

    @Mock
    private ProductService productService;

    private ProductRequestDto productRequestDto;
    private ProductResponseDto productResponseDto;
    private UserDetailsImpl userDetails;

    @BeforeEach
    void setUp(){

        User user = new User("test12345@naver.com","aaa111!", Role.USER,"test","010-1111-1111");

        userDetails = new UserDetailsImpl(user);
        UsernamePasswordAuthenticationToken auth =
                new UsernamePasswordAuthenticationToken(userDetails, null, userDetails.getAuthorities());

        SecurityContext securityContext = SecurityContextHolder.createEmptyContext();
        securityContext.setAuthentication(auth);
        SecurityContextHolder.setContext(securityContext);

        productRequestDto = new ProductRequestDto("이름","설명","");
        productResponseDto = new ProductResponseDto();

        ReflectionTestUtils.setField(productResponseDto,"userId",1L);
        ReflectionTestUtils.setField(user,"id",1L);


    }

    @Test
    @DisplayName("상품추가 테스트")
    void addProduct() {
        when(productService.addProduct(1L,productRequestDto)).thenReturn(productResponseDto);

        ResponseEntity<CommonResponseBody<ProductResponseDto>> response = productController.addProduct(userDetails,productRequestDto);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(productResponseDto, Objects.requireNonNull(response.getBody()).getData());
        verify(productService,times(1)).addProduct(userDetails.getUser().getId(),productRequestDto);

    }

    @Test
    void getProduct() {
    }

    @Test
    void updateProduct() {
    }

    @Test
    void deleteProduct() {
    }
}