package ru.abenefic.webapp;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@WebServlet(name = "ProductServlet", urlPatterns = "/product")
public class ProductServlet extends HttpServlet {

    private List<Product> productList;

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        for (Product product : productList) {
            resp.getWriter().println(product);
        }
    }

    @Override
    public void init() {
        Random random = new Random();
        productList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            productList.add(Product.builder()
                    .id(i)
                    .title("Product " + i)
                    .cost((float) Math.abs(random.nextInt(100) * 1.4))
                    .build());
        }
    }
}