package com.upgrad.FoodOrderingApp.api.controller;


import com.upgrad.FoodOrderingApp.api.model.CategoriesListResponse;
import com.upgrad.FoodOrderingApp.api.model.CategoryDetailsResponse;
import com.upgrad.FoodOrderingApp.api.model.CategoryListResponse;
import com.upgrad.FoodOrderingApp.api.model.ItemList;
import com.upgrad.FoodOrderingApp.service.business.CategoryService;
import com.upgrad.FoodOrderingApp.service.business.ItemService;
import com.upgrad.FoodOrderingApp.service.entity.CategoryEntity;
import com.upgrad.FoodOrderingApp.service.entity.ItemEntity;
import com.upgrad.FoodOrderingApp.service.exception.CategoryNotFoundException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

/**
 * @author zeelani,vipin,madhuri,ashish
 * Category Controller Handles all  the Category related endpoints
 */

@RestController
@RequestMapping("/category")
public class CategoryController {

    // injecting the service logic handling classes
    @Autowired
    CategoryService categoryService;

    /**
     * Get method to retrieve all categories
     * @return CategoriesListResponse
     */
    @CrossOrigin
    @RequestMapping(method = RequestMethod.GET,path = "",produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public ResponseEntity<CategoriesListResponse> getAllCategories(){

        // calling the service logic function to retrieve all categories
        List<CategoryEntity> categoryEntities = categoryService.getAllCategories();

        // proceed to check only if the retrieved categories are non empty
        if(!categoryEntities.isEmpty()) {
            List<CategoryListResponse> categoryListResponses = new LinkedList<>();
            // Looping through the categories received.
            categoryEntities.forEach(categoryEntity -> {
                // Creating Category List response to add  it to the list.
                CategoryListResponse categoryListResponse = new CategoryListResponse()
                        .id(UUID.fromString(categoryEntity.getUuid()))
                        .categoryName(categoryEntity.getCategoryName());
                categoryListResponses.add(categoryListResponse);
            });

            // generating the final response using the above category list responses
            CategoriesListResponse categoriesListResponse = new CategoriesListResponse().categories(categoryListResponses);
            return new ResponseEntity<CategoriesListResponse>(categoriesListResponse, HttpStatus.OK);
        }else{
            // creating the empty response
            return new ResponseEntity<CategoriesListResponse>(new CategoriesListResponse(),HttpStatus.OK);
        }
    }

    /**
     * Method to get  category by its id
     * @param categoryUuid
     * @return CategoryDetailsResponse
     * @throws CategoryNotFoundException
     */
    @CrossOrigin
    @RequestMapping(method = RequestMethod.GET,path = "/{category_id}",produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public ResponseEntity<CategoryDetailsResponse> getCategoryById(@PathVariable(value = "category_id")final String categoryUuid)
            throws CategoryNotFoundException {

        // Calls get Category By Id method of categoryService to get the CategoryEntity.
        CategoryEntity categoryEntity = categoryService.getCategoryById(categoryUuid);

        //Creating list of itemEntities corresponding to the categoryEntities.
        List<ItemEntity> itemEntities = categoryEntity.getItems();

        //Creating List of ItemList
        List<ItemList> itemLists = new LinkedList<>();
        itemEntities.forEach(itemEntity -> {
            ////Looping in for each itemEntity in itemEntities and
            ItemList itemList = new ItemList()
                    .id(UUID.fromString(itemEntity.getUuid()))
                    .price(itemEntity.getPrice())
                    .itemName(itemEntity.getitemName())
                    .itemType(ItemList.ItemTypeEnum.fromValue(itemEntity.getType().getValue()));
            itemLists.add(itemList);
        });

        //Creating CategoryDetailsResponse by adding the itemList and other details.
        CategoryDetailsResponse categoryDetailsResponse = new CategoryDetailsResponse()
                .categoryName(categoryEntity.getCategoryName())
                .id(UUID.fromString(categoryEntity.getUuid()))
                .itemList(itemLists);
        return new ResponseEntity<CategoryDetailsResponse>(categoryDetailsResponse,HttpStatus.OK);
    }

}
