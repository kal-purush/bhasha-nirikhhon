package market.controller;

import market.domain.Category;
import market.repository.CategoryRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

@RestController
@RequestMapping("/categories")
public class CategoryController {

    private final CategoryRepository categoryRepository;

    @Autowired
    public CategoryController(CategoryRepository categoryRepository) {
        this.categoryRepository = categoryRepository;
    }

    @RequestMapping(method = RequestMethod.GET)
    public Iterable<Category> readCategories(){
        return categoryRepository.findAll();
    }

    @RequestMapping(value = "/{id}",method = RequestMethod.GET)
    Category readCategoryById(@PathVariable Long id){
        return categoryRepository.findOne(id);
    }

    @RequestMapping(value = "?name&description",method = RequestMethod.POST)
    ResponseEntity<?> create(@PathVariable String name, @PathVariable String description){
        return this.categoryRepository
                .findAllByName(name)
                .map(category -> {
                    Category result = categoryRepository.save(new Category(name, description));

                    HttpHeaders httpHeaders = new HttpHeaders();
                    httpHeaders.setLocation(ServletUriComponentsBuilder
                            .fromCurrentRequest().path("/{id}")
                            .buildAndExpand(result.getId()).toUri());
                    return new ResponseEntity<>(null, httpHeaders, HttpStatus.CREATED);
                }).get();
    }

}