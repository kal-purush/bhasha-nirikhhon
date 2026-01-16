package by.ras.impl;

import by.ras.BaseTest;
import by.ras.ProductService;
import by.ras.entity.ProductType;
import by.ras.entity.particular.Product;

import by.ras.repository.ProductRepository;
import lombok.extern.log4j.Log4j;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.transaction.annotation.Transactional;

import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.*;

@Log4j
@Transactional
public class ProductServiceTest extends BaseTest {

    @Autowired
    ProductRepository productRepository;
    @Autowired
    ProductService productService;

    @Test
    public void add() throws Exception {
        initProducts();
        Product template = new Product("aa", "aa", "aa", "aa", "1", "dd");
        Product product = productService.add(template);
        Product foundProduct = productRepository.findByCompanyAndProductNameAndModelAndType(
                "aa", "aa", "aa", "aa");
        assertEquals(product, foundProduct);
    }

    @Test
    public void findById() throws Exception {
        initProducts();
        Product foundProduct = productService.findById(1L);
        Product product = productRepository.findOne(1L);
        assertEquals(product, foundProduct);
    }

    @Test
    public void findAll() throws Exception {
        initProducts();
        int PAGE = 0;
        int OBJ_PER_PAGE = 3;
        PageRequest pageRequest = new PageRequest(PAGE, OBJ_PER_PAGE);
        List<Product> foundProducts = productService.findAll(pageRequest);
        List<Product> products = new LinkedList<>();
        for(long i = (PAGE * OBJ_PER_PAGE); i < ((PAGE + 1) * OBJ_PER_PAGE); i++){
            products.add(productRepository.findOne(i+1));
        }
        assertEquals(products, foundProducts);
    }

    @Test
    public void findAllComplex() throws Exception {
        initProducts();
        int PAGE = 0;
        int OBJ_PER_PAGE = 3;
        PageRequest pageRequest = new PageRequest(PAGE, OBJ_PER_PAGE);
        Product filter = Product.builder().price("1").build();
        List<Product> foundProducts = productService.findAllComplex(filter, new PageRequest(0, 5));
        Product product = productRepository.findOne(1L);
        List<Product> products = new LinkedList<>();
        products.add(product);
        assertEquals(products, foundProducts);

    }

    @Test
    public void update() throws Exception {
        initProducts();
        Product toChange = productRepository.findOne(1L);
        toChange.setCompany("aa");
        toChange.setProductName("aa");
        toChange.setModel("aa");
        toChange.setProductType(ProductType.TV.name());
        toChange.setPrice("22");
        toChange.setDescription("didi");
        Product product = productService.update(toChange);
        Product foundProduct = productRepository.findOne(1L);
        assertEquals(product, foundProduct);
    }

    @Test
    public void delete() throws Exception {
        initProducts();
        Product check = productRepository.findOne(1L);
        productService.delete(check.getId());
        Product foundProduct = productRepository.findOne(check.getId());
        assertEquals(null, foundProduct);
    }

    @Test
    public void countRows() throws Exception {
        initProducts();
        long foundRows = productService.countRows();
        long rows = productRepository.findAll().size();
        assertEquals(rows, foundRows);
    }

    @Test
    public void countRowsComplex() throws Exception {
        Object[] init = initProducts();
        Product filter = new Product();
        filter.setProductType(((String []) init[1])[1]);
        long foundRows = productService.countRowsComplex(filter);
        long rows = ((String []) init[0]).length;
        assertEquals(rows, foundRows);
    }

    private Object[] initProducts() throws Exception {
        String[] companies = {"Apple", "Samsung", "XiaoMi", "Huawei", "Microsoft", "Dell", "Asus"};
        String[] types = {ProductType.CELLPHONE.name(), ProductType.LAPTOP.name(), ProductType.TV.name(),
                ProductType.TABLET.name(), ProductType.MP3.name(), ProductType.DISPLAY.name(),};
        List<Product> products = new LinkedList<>();
        for (int i = 0; i < companies.length; i++) {
            for (int m = 0; m < types.length; m++) {
                if (productRepository.findByCompanyAndProductNameAndModelAndType(companies[i], "any_name",
                        companies[i].toLowerCase().concat("-" + m), types[m]) == null) {
                    Product product = Product.builder()
                            .company(companies[i])
                            .productName("any_name")
                            .model(companies[i].toLowerCase().concat("-" + m))
                            .productType(types[m])
                            .price(String.valueOf((i + 1) * (m + 1)))
                            .description("any_text")
                            .build();
                    products.add(product);
                    productRepository.saveAndFlush(product);
                }
            }
        }
        Object[] result = new Object[3];
        result[0] = companies;
        result[1] = types;
        result[2] = products;
        return result;
    }

}