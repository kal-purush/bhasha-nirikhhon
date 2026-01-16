package ru.otus.spring.hw10_rest_book_info_app.controller.rest;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.otus.spring.hw10_rest_book_info_app.controller.rest.dto.BookDto;
import ru.otus.spring.web_ui_book_info_app.service.book.BookService;
import ru.otus.spring.web_ui_book_info_app.service.book.info.get.GetBookInfoService;

import java.util.List;
import static java.util.Collections.emptyList;

@RestController
@RequiredArgsConstructor
public class BookInfoController {
    private final BookService bookService;
    private final ResultHandler resultHandler;

    @GetMapping("/books")
    public List<BookDto> bookInfoList() {
        var serviceResult = bookService.getAll();

        return
            resultHandler
                .handle(serviceResult, emptyList(), emptyList())
            ;
    }
}