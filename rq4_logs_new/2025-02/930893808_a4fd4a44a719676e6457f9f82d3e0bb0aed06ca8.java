package id.ac.ui.cs.advprog.eshop.controller;

import id.ac.ui.cs.advprog.eshop.model.Product;
import id.ac.ui.cs.advprog.eshop.service.ProductService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.ui.Model;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class ProductControllerTest {

    private MockMvc mockMvc;

    @Mock
    private ProductService service;

    @Mock
    private Model model;

    @InjectMocks
    private ProductController controller;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(controller).build();
    }

    @Test
    void testCreateProductPage() {
        String view = controller.createProductPage(model);
        verify(model).addAttribute(eq("product"), any(Product.class));
        assertEquals("createProduct", view);
    }

    @Test
    void testCreateProductPost() throws Exception {
        Product product = new Product();
        mockMvc.perform(post("/product/create").flashAttr("product", product))
                .andExpect(status().is3xxRedirection())
                .andExpect(redirectedUrl("list"));
        verify(service).create(product);
    }

    @Test
    void testProductListPage() {
        List<Product> products = Arrays.asList(new Product(), new Product());
        when(service.findAll()).thenReturn(products);

        String view = controller.productListPage(model);
        verify(model).addAttribute("products", products);
        assertEquals("productList", view);
    }

    @Test
    void testEditProductPage() {
        Product product = new Product();
        when(service.findProductById("1")).thenReturn(product);

        String view = controller.editProductPage("1", model);
        verify(model).addAttribute("product", product);
        assertEquals("editProduct", view);
    }

    @Test
    void testEditProductPost() throws Exception {
        Product product = new Product();
        mockMvc.perform(post("/product/edit").flashAttr("product", product))
                .andExpect(status().is3xxRedirection())
                .andExpect(redirectedUrl("list"));
        verify(service).update(product);
    }

    @Test
    void testDeleteProduct() throws Exception {
        Product product = new Product();
        when(service.findProductById("1")).thenReturn(product);

        mockMvc.perform(delete("/product/delete/1"))
                .andExpect(status().is3xxRedirection())
                .andExpect(redirectedUrl("../list"));
        verify(service).delete(product);
    }
}