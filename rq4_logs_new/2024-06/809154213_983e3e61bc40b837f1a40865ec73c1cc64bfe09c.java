package de.ait_tr.g_40_shop.logging;

import de.ait_tr.g_40_shop.domain.dto.ProductDto;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;

@Aspect
@Component
public class AspectLogging {

    private Logger logger = LoggerFactory.getLogger(AspectLogging.class);

    // Pointcut (срез)
    @Pointcut("execution(* de.ait_tr.g_40_shop.service.ProductServiceImpl.save(..))")
    public void saveProduct() { // метод только для задания Pointcut
    }

    // Before
//    @Before("saveProduct()")
//    public void beforeSavingProduct() {
//        logger.info("Method save of the class ProductServiceImpl called");
//    }

    @Before("saveProduct()")
    public void beforeSavingProduct(JoinPoint joinPoint) {
        Object[] params = joinPoint.getArgs();
        logger.info("Method save of the class ProductServiceImpl called with parameter {}", params[0]);
    }
    @After("saveProduct()")
    public void afterSavingProduct() {
        logger.info("Method save of the class ProductServiceImpl finished its work");
    }

    @Pointcut("execution(* de.ait_tr.g_40_shop.service.ProductServiceImpl.getById(..))")
    public void getProductById() {}

//    @AfterReturning("getProductById()")
//    public void afterReturningProduct(){
//        logger.info("Method getById of the class ProductServiceImpl successfully returned result");
//    }
//
//    @AfterThrowing("getProductById()")
//    public void afterThrowingIfProductNotFound() {
//        logger.warn("Method getById of the class ProductServiceImpl threw an exception");
//    }

    @AfterReturning(
            pointcut = "getProductById()",
            returning = "result"
    )
    public void afterReturningProduct(Object result) {
        logger.info("Method getById of the class ProductServiceImpl successfully returned result {}", result);
    }

    @AfterThrowing(
            pointcut = "getProductById()",
            throwing = "e"
    )
    public void afterThrowingIfProductNotFound(Exception e) {
        logger.warn("Method getById of the class ProductServiceImpl threw an exception: {}", e.getMessage());
    }

    @Pointcut("execution(* de.ait_tr.g_40_shop.service.ProductServiceImpl.getAllActiveProducts(..))")
    public void getAllProducts() {}

    @Around("getAllProducts()")
    public Object aroundGettingAllProducts(ProceedingJoinPoint joinPoint) {
        logger.info("Method getAllActiveProducts of the class ProductServiceImpl called");

        List<ProductDto> result = null;

        try {
            result = ((List<ProductDto>) joinPoint.proceed())
                    .stream()
                    .filter(x -> x.getPrice().intValue() > 100)
                    .toList();
        } catch (Throwable e) {
            logger.error("Method getAllActiveProducts of the class ProductServiceImpl threw an exception: {}", e.getMessage());
        }

        logger.info("Method getAllActiveProducts of the class ProductServiceImpl finished its work with result: {}", result);
        return result;
    }
}