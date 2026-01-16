package treeptik.ldallen.controller.todolist;

import com.github.aesteve.vertx.nubes.annotations.Controller;
import com.github.aesteve.vertx.nubes.annotations.File;
import com.github.aesteve.vertx.nubes.annotations.View;
import com.github.aesteve.vertx.nubes.annotations.params.Param;
import com.github.aesteve.vertx.nubes.annotations.routing.http.GET;
import com.github.aesteve.vertx.nubes.annotations.routing.http.PUT;
import com.github.aesteve.vertx.nubes.annotations.services.Service;
import com.github.aesteve.vertx.nubes.context.FileResolver;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import treeptik.ldallen.domains.Product;
import treeptik.ldallen.services.ProductService;

@Controller("/")
public class ProductsController {

    @Service("productService")
    private ProductService products;

    @GET
    public void indexView(RoutingContext context){
        context.response().end("This is a REST example using Vert.x Nubes framework");
    }
    @GET("products")
    public void getProducts(RoutingContext context) {
        context.response().end(products.getProducts().toString());
    }

    @GET("products/:id")
    public void getProduct(RoutingContext context, @Param("id") String id) {
        Product prod = products.getProduct(Integer.parseInt(id));
        context.response().end(prod.toString());
    }

    @PUT("products/:id")
    public void putProduct(RoutingContext context, @Param("id") String id) {
        JsonObject json = context.getBodyAsJson();
        Product product = new Product(json.getString("name"),json.getDouble("price"),json.getInteger("weight"));
        products.add(Integer.parseInt(id),product);
        context.response().end("product added!");
    }


}