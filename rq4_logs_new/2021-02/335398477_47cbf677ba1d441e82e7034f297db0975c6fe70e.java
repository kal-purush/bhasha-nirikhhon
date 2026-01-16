package pl.khamul.handworkshop.Session;




import pl.khamul.handworkshop.entity.CartItem;
import pl.khamul.handworkshop.entity.Product;
import pl.khamul.handworkshop.entity.ShoppingCart;
import pl.khamul.handworkshop.repository.ProductRepository;


import javax.servlet.annotation.WebListener;
import javax.servlet.http.HttpSessionEvent;
import javax.servlet.http.HttpSessionListener;


@WebListener
public class SessionListener implements HttpSessionListener {
    ProductRepository productRepository;




    @Override
    public void sessionDestroyed(HttpSessionEvent se) {


        ShoppingCart cart = (ShoppingCart) se.getSession().getAttribute("cart");


        for(CartItem x : cart.getItems()){
            Product product = x.getProduct();
            product.setStoragequantity(product.getStoragequantity() + x.getQuantity());
            System.out.println(product);
            productRepository.save(product);
        }



        System.out.println("session killed");

    }
}