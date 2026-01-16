package com.pragma.webflux.api.controller;

import com.pragma.webflux.api.model.Category;
import com.pragma.webflux.api.model.Image;
import com.pragma.webflux.api.model.Product;
import com.pragma.webflux.api.service.IProductService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.http.codec.multipart.FormFieldPart;
import org.springframework.stereotype.Component;
import org.springframework.validation.BeanPropertyBindingResult;
import org.springframework.validation.Errors;
import org.springframework.validation.Validator;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.File;
import java.net.URI;
import java.util.Date;
import java.util.UUID;

import static org.springframework.web.reactive.function.BodyInserters.fromValue;

@Component
public class ProductHandler {

    @Autowired
    private IProductService productService;

    @Value("${images.upload.path}")
    private String imagesPath;

    @Autowired
    private Validator validator;

    public Mono<ServerResponse> list() {
        return ServerResponse
                .ok()
                .contentType(MediaType.APPLICATION_JSON)
                .body(productService.findAll(), Product.class);
    }

    public Mono<ServerResponse> getProduct(ServerRequest request) {
        return productService
                .findById(request.pathVariable("id"))
                .flatMap(p ->
                        ServerResponse
                                .ok()
                                .contentType(MediaType.APPLICATION_JSON)
                                .body(fromValue(p)))
                .switchIfEmpty(
                        ServerResponse
                                .notFound()
                                .build());
    }

    public Mono<ServerResponse> createProduct(ServerRequest request) {
        return request.bodyToMono(Product.class)
                .flatMap(p -> {
                    Errors errors = new BeanPropertyBindingResult(p, Product.class.getName());
                    validator.validate(p, errors);
                    if (errors.hasErrors()) {
                        return Flux
                                .fromIterable(errors.getFieldErrors())
                                .map(fieldError ->
                                        "Field: '" + fieldError.getField() + "', " + fieldError.getDefaultMessage())
                                .collectList()
                                .flatMap(list -> ServerResponse
                                        .badRequest()
                                        .body(fromValue(list)));
                    } else {
                        if (p.getCreateAt() == null) {
                            p.setCreateAt(new Date());
                        }
                        return productService.save(p)
                                .flatMap(pdb -> ServerResponse
                                        .created(URI.create(pdb.getId()))
                                        .contentType(MediaType.APPLICATION_JSON)
                                        .body(fromValue(pdb)));
                    }
                });
    }

    public Mono<ServerResponse> editProduct(ServerRequest request) {
        return productService
                .findById(request.pathVariable("id"))
                .flatMap(p ->
                        request.bodyToMono(Product.class)
                                .flatMap(product -> {
                                    p.setName(product.getName());
                                    p.setPrice(product.getPrice());
                                    p.setCategory(product.getCategory());
                                    return productService.save(p);
                                })
                )
                .flatMap(p ->
                        ServerResponse
                                .ok()
                                .contentType(MediaType.APPLICATION_JSON)
                                .body(fromValue(p)))
                .switchIfEmpty(
                        ServerResponse
                                .notFound()
                                .build());
    }

    public Mono<ServerResponse> editProduct2(ServerRequest request) {
        return productService
                .findById(request.pathVariable("id"))
                .zipWith(request.bodyToMono(Product.class), (db, req) ->
                {
                    db.setName(req.getName());
                    db.setPrice(req.getPrice());
                    db.setCategory(req.getCategory());
                    return db;
                })
                .flatMap(p ->
                        ServerResponse
                                .ok()
                                .contentType(MediaType.APPLICATION_JSON)
                                .body(productService.save(p), Product.class))
                .switchIfEmpty(
                        ServerResponse
                                .notFound()
                                .build());
    }

    public Mono<ServerResponse> deleteProduct(ServerRequest request) {
        return productService
                .findById(request.pathVariable("id"))
                .flatMap(p -> productService.delete(p)
                        .then(ServerResponse
                                .noContent()
                                .build()))
                .switchIfEmpty(
                        ServerResponse
                                .notFound()
                                .build());
    }

    public Mono<ServerResponse> uploadImage(ServerRequest request) {

        return request.multipartData()
                .map(m -> m.toSingleValueMap().get("file"))
                .cast(FilePart.class)
                .flatMap(file ->
                        productService
                                .findById(request.pathVariable("id"))
                                .flatMap(p -> {
                                    String filename = UUID.randomUUID() + "-" + file.filename()
                                            .replace(" ", "")
                                            .replace(":", "")
                                            .replace("\\", "");
                                    if (!file.filename().isEmpty()) {
                                        Image image = new Image(filename);
                                        p.getImages().add(image);
                                    }
                                    return file.transferTo(new File(imagesPath + filename))
                                            .then(productService.save(p));
                                }))
                .flatMap(p ->
                        ServerResponse
                                .created(URI.create("images"))
                                .body(fromValue(p)))
                .switchIfEmpty(
                        ServerResponse
                                .notFound()
                                .build());
    }

    public Mono<ServerResponse> createWithImage(ServerRequest request) {
        Mono<Product> product = request.multipartData()
                .map(multipart -> {
                    String name = ((FormFieldPart) multipart.toSingleValueMap().get("name")).value();
                    String price = ((FormFieldPart) multipart.toSingleValueMap().get("price")).value();
                    String categoryId = ((FormFieldPart) multipart.toSingleValueMap().get("category.id")).value();
                    String categoryName = ((FormFieldPart) multipart.toSingleValueMap().get("category.name")).value();
                    Category category = new Category(categoryName);
                    category.setId(categoryId);
                    return new Product(name, Double.parseDouble(price), category);
                });
        return request.multipartData()
                .map(m -> m.toSingleValueMap().get("file"))
                .cast(FilePart.class)
                .flatMap(file -> product
                        .flatMap(p -> {
                            String filename = UUID.randomUUID() + "-" + file.filename()
                                    .replace(" ", "")
                                    .replace(":", "")
                                    .replace("\\", "");
                            if (!file.filename().isEmpty()) {
                                Image image = new Image(filename);
                                p.getImages().add(image);
                            }
                            return file.transferTo(new File(imagesPath + filename))
                                    .then(productService.save(p));
                        }))
                .flatMap(p ->
                        ServerResponse
                                .created(URI.create("images"))
                                .body(fromValue(p)));
    }
}