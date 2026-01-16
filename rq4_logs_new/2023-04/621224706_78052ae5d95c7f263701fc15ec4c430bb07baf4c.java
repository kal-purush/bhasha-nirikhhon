package JPABook.JPAShop.controller;

import JPABook.JPAShop.controller.form.BookForm;
import JPABook.JPAShop.domain.Item.Book;
import JPABook.JPAShop.domain.Item.Item;
import JPABook.JPAShop.service.ItemService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;

import java.util.List;

@Controller
public class ItemController {
    @Autowired
    private final ItemService itemService;

    public ItemController(ItemService itemService) {
        this.itemService = itemService;
    }

    @GetMapping("/items/new")
    public String createForm(Model model) {
        model.addAttribute("form", new BookForm()); //model 에 대한 입력값 세팅이 잘못되었을 경우 "template parsing err" 를 뱉어낸다.
        System.out.println("hh");
        return "items/createItemForm";
    }

    @PostMapping("/items/new")
    public String create(BookForm form) {
        Book book = new Book();
        book.setName(form.getName());
        book.setPrice(form.getPrice());
        book.setStockQuantity(form.getStockQuantity());
        book.setAuthor(form.getAuthor());
        book.setIsbn(form.getIsbn());

        itemService.saveItem(book);
        return "redirect:/";
    }

    @GetMapping("/items")
    public String list(Model model) {
        List<Item> items = itemService.findAll();
        model.addAttribute("items", items);
        return "items/itemList.html";
    }

    //pathVariable
    @GetMapping("items/{itemId}/edit")
    public String updateItemForm(@PathVariable("itemId") Long itemId, Model model) {
        Book findBook = (Book) itemService.findOne(itemId);

        BookForm bookForm = new BookForm();
        bookForm.setId(findBook.getId());
        bookForm.setName(findBook.getName());
        bookForm.setPrice(findBook.getPrice());
        bookForm.setStockQuantity(findBook.getStockQuantity());
        bookForm.setAuthor(findBook.getAuthor());
        bookForm.setIsbn(findBook.getIsbn());

        model.addAttribute("form", bookForm);

        return "items/updateItemForm.html";
    }

    @PostMapping("items/{itemId}/edit")
    public String updateItem(@ModelAttribute("form") BookForm bookForm) { //@ModelAttribute thymrif 템플릿에서 넘어오는 데이터를 매핑 해주는 어노테이션
        //update
        // 어떻게 영속성 컨텍스트로 넘겨서 값의 변동을 받을 것인가?
        // 1. repo 에서 조회해서 가져와서 Book 영속성 객체를 가지고 있어
        // 2. 수정해줘 (set ...)
        // 3. 그냥 transacional 에서 commit 되서 수정되게
        itemService.update(bookForm);
        return "redirect:/items";
    }

}