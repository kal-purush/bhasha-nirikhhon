package ru.otus.spring.homework13.controllers;

import lombok.Data;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import ru.otus.spring.homework13.domain.Author;
import ru.otus.spring.homework13.security.MyAclService;
import ru.otus.spring.homework13.service.AuthorService;

import java.util.List;

@Data
@Controller
public class ControllerForAuthor {

    private final AuthorService authorService;
    private final MyAclService myAclService;

    @GetMapping("/author/id={id}")
    public String getAuthorById(@PathVariable("id") long id, Model model) {
        Author author = authorService.showById(id);
        model.addAttribute("author", author);
        return "authorList";
    }

    @GetMapping("/author")
    public String getAuthorAll(Model model) {
        List<Author> authors = authorService.showAllRows();
        model.addAttribute("author", authors);
        model.addAttribute("adminmode", turnAdminMode());
        return "authorList";
    }

    @PostMapping("/author/add")
    public String insertAuthor(Author author, Model model) {
        String result = authorService.insert(author);
        model.addAttribute("result", result);
        return "resultInsertAuthor";
    }

    @GetMapping("/author/insertauthor")
    public String getPageInsert(Model model) {
        Author form = new Author();
        model.addAttribute("author", form);
        return "insertAuthor";
    }

    @PostMapping("/author/del")
    public String deleteAuthorById(@RequestParam("id") long id, Model model) {
        String result = authorService.deleteById(id);
        model.addAttribute("result", result);
        return "deleteAuthor";
    }

    private boolean turnAdminMode() {
        try {
            return myAclService.isAdmin();
        } catch (AccessDeniedException e) {
            return false;
        }
    }
}