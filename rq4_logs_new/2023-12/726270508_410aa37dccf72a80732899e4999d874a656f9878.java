package work.winksoft.book.controllers;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;

import work.winksoft.book.data.BookDAO;
import work.winksoft.book.entities.Book;

@Controller
public class BookController {

	@Autowired
	private BookDAO bookDAO;
	
	@RequestMapping(path = { "/", "home.do" })
	public String showAllBooks(Model model) {
		List<Book> books = bookDAO.getAllBooks();
		model.addAttribute("books", books);
		return "home";
	}
}