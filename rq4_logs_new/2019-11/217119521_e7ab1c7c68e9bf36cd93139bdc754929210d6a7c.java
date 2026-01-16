package com.es.phoneshop.cart;

import com.es.phoneshop.model.product.Product;

import javax.servlet.http.HttpSession;
import java.util.Queue;

public class RecentlyViewedProductsService {
    private static RecentlyViewedProductsService recentlyViewedProductsService;

    private RecentlyViewedProductsService() {
    }

    public static RecentlyViewedProductsService getInstance() {
        if (recentlyViewedProductsService == null) {
            recentlyViewedProductsService = new RecentlyViewedProductsService();
        }
        return recentlyViewedProductsService;
    }

    public void setRecentlyViewedProductInSession(HttpSession session) {
        RecentlyViewedProducts recentlyViewedProducts = (RecentlyViewedProducts) session.getAttribute("recentlyViewedProducts");
        if (recentlyViewedProducts == null) {
            recentlyViewedProducts = new RecentlyViewedProducts();
        }
        session.setAttribute("recentlyViewedProducts",recentlyViewedProducts);
    }

    public void addProductInRecentlyViewes(Product product, HttpSession session) {
        RecentlyViewedProducts recentlyViewedProducts = (RecentlyViewedProducts) session.getAttribute("recentlyViewedProducts");
        Queue<Product> productQueue = recentlyViewedProducts.getProductQueue();
        if (productQueue.contains(product)) {
            productQueue.remove(product);
            productQueue.add(product);
        }else {
            if (productQueue.size() < 3) {
                productQueue.add(product);
            } else {
                productQueue.poll();
                productQueue.add(product);
            }
        }
        recentlyViewedProducts.setProductQueue(productQueue);
        session.setAttribute("recentlyViewedProducts",recentlyViewedProducts);
    }
}