package internet.market.controllers;

import internet.market.lib.Injector;
import internet.market.model.Product;
import internet.market.service.ProductService;
import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class AddProductController extends HttpServlet {
    private static final Injector injector = Injector.getInstance("internet.market");
    private ProductService productService = (ProductService) injector
            .getInstance(ProductService.class);

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        req.getRequestDispatcher("/WEB-INF/views/addProduct.jsp")
                .forward(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        String productName = req.getParameter("name");
        String productPrice = req.getParameter("price");
        if (productName.length() > 0 && productPrice.length() > 0) {
            Product product = new Product(productName, Long.parseLong(productPrice));
            productService.create(product);
            resp.sendRedirect(req.getContextPath() + "/");
        } else {
            req.setAttribute("message", "You entered wrong name or price");
            req.getRequestDispatcher("/WEB-INF/views/addProduct.jsp").forward(req, resp);
        }
    }
}