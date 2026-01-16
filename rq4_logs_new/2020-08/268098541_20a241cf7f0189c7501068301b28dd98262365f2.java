package ru.otus.spring.rest_book_info_app.controller.page;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import ru.otus.spring.rest_book_info_app.controller.rest.dto.BookDto;

@Controller
public class BookPageController {
    @GetMapping("/")
    public String books() {
        return "book/list";
    }

    @GetMapping("/addBook")
    public String add(Model model) {
        model.addAttribute("book", new BookDto());

        return "book/add";
    }

    @GetMapping("/editBook")
    public String edit(@RequestParam("id") String id, @RequestParam("title") String title, Model model) {
        model
            .addAttribute("book", new BookDto())
            .addAttribute("id", id)
            .addAttribute("title", title)
        ;

        return "book/edit";
    }
}