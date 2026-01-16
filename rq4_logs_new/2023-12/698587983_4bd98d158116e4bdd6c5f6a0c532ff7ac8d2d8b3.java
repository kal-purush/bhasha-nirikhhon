package ru.otus.marchenko.controllers;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import ru.otus.marchenko.dto.author.AuthorCreateDto;
import ru.otus.marchenko.services.AuthorService;

@Controller
@RequiredArgsConstructor
public class AuthorController {

    private final AuthorService service;

    @GetMapping("/author/add")
    public String addNewAuthor(AuthorCreateDto authorCreateDto, Model model) {
        model.addAttribute("authorCreateDto", authorCreateDto);
        return "author/add";
    }

    @PostMapping("/author/add")
    public String saveNewAuthor(@Valid AuthorCreateDto authorCreateDto,
                                BindingResult bindingResult, Model model) {
        if (bindingResult.hasErrors()) {
            return "/author/add";
        }
        service.insert(authorCreateDto);
        return "redirect:../book/add";
    }
}