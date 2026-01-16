package com.olehedza.reviews.controller;

import com.olehedza.reviews.dto.parser.CsvDto;
import com.olehedza.reviews.mapper.CsvMapper;
import com.olehedza.reviews.model.Product;
import com.olehedza.reviews.model.Review;
import com.olehedza.reviews.model.User;
import com.olehedza.reviews.service.ProductService;
import com.olehedza.reviews.service.ReviewService;
import com.olehedza.reviews.service.UserService;
import com.olehedza.reviews.util.Parser;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.List;
import lombok.AllArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RequestMapping("/inject")
@RestController
@AllArgsConstructor
public class InjectCsvToDbController {
    private static final String PATH = "reviews_mini.csv";
    private final UserService userService;
    private final ReviewService reviewService;
    private final ProductService productService;
    private final CsvMapper mapper;
    private final Parser<CsvDto> parser;

    @GetMapping
    public HttpStatus injectCsvData(@RequestParam(defaultValue = PATH) String path) {
        List<CsvDto> parse;
        try {
            parse = parser.parse(path);
            for (CsvDto dto: parse) {
                Review review = reviewService.addReview(mapper.toReview(dto));
                Product product = productService.addProduct(mapper.toProduct(dto, review));
                User user = mapper.toUser(dto, review, product);
                user.getProducts().add(product);
                user.getReviews().add(review);
                userService.addUser(user);
            }
            return HttpStatus.OK;
        } catch (IOException e) {
            throw new UndeclaredThrowableException(e, "Can't parse csv file");
        }
    }
}