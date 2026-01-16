package com.upgrad.FoodOrderingApp.api.controller;

import com.upgrad.FoodOrderingApp.api.model.*;
import com.upgrad.FoodOrderingApp.service.business.CategoryService;
import com.upgrad.FoodOrderingApp.service.business.RestaurantService;
import com.upgrad.FoodOrderingApp.service.entity.CategoryEntity;
import com.upgrad.FoodOrderingApp.service.entity.RestaurantEntity;
import com.upgrad.FoodOrderingApp.service.exception.CategoryNotFoundException;
import com.upgrad.FoodOrderingApp.service.exception.RestaurantNotFoundException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

/**
 * @author Vipin Mohan
 * Controller class for handling the Restaurant endpoint requests
 */
@RestController
@RequestMapping("/")
public class RestaurantController {

   @Autowired
   private RestaurantService restaurantService;
    @Autowired
    private CategoryService categoryService;

    /**
     * GET method for handling get all restaurant request
     * @return restaurantListResponse
     */
    @CrossOrigin
    @RequestMapping(method = RequestMethod.GET, path = "restaurant", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<RestaurantListResponse> restaurantsByRating() throws CategoryNotFoundException {
        List<RestaurantEntity> allRestaurant = restaurantService.getAllRestaurant();

        RestaurantListResponse restaurantListResponse = new RestaurantListResponse().restaurants((List<RestaurantList>) fetchDetails(allRestaurant));
        return new ResponseEntity<RestaurantListResponse>(restaurantListResponse, HttpStatus.OK);
    }
    /**
     * GET method for handling get restaurant based on restaurant name request
     * @param restaurantName
     * @return restaurantListResponse
     */
    @CrossOrigin
    @RequestMapping(method = RequestMethod.GET, path = "/restaurant/name/{restaurant_name}",
            produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public ResponseEntity<RestaurantListResponse> restaurantsByName(
            @PathVariable("restaurant_name") final String restaurantName)
            throws RestaurantNotFoundException, CategoryNotFoundException {
        List<RestaurantEntity> foundRestaurant = restaurantService.getRestaurantByName(restaurantName);
        RestaurantListResponse restaurantListResponse = new RestaurantListResponse().restaurants(fetchDetails(foundRestaurant));
        return new ResponseEntity<RestaurantListResponse>(restaurantListResponse, HttpStatus.OK);
    }
    /**
     * GET method for handling get restaurant based on restaurant id request
     * @param restaurant_id
     * @return restaurantListResponse
     */
    @CrossOrigin
    @RequestMapping(method = RequestMethod.GET, path = "/api/restaurant/{restaurant_id}",
            produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public ResponseEntity<RestaurantListResponse> restaurantByUUID(
            @PathVariable("restaurant_id") final String restaurant_id)
            throws RestaurantNotFoundException, CategoryNotFoundException {

        RestaurantEntity restaurantEntities = restaurantService.getRestaurantByUuid(restaurant_id);

        /*Get Restaurant categories */
        List<CategoryEntity> categoryEntities = categoryService.getCategoriesByRestaurant(restaurantEntities.getUuid());
        String categories = new String();
        //To concat the category names.
        ListIterator<CategoryEntity> listIterator = categoryEntities.listIterator();
        while (listIterator.hasNext()) {
            categories = categories + listIterator.next().getCategoryName();
            if (listIterator.hasNext()) {
                categories = categories + ", ";
            }
        }
        List<RestaurantList> restaurantDetailsResponseList = new ArrayList<>();

        List<RestaurantList> restaurantLists = new LinkedList<>();
        RestaurantList restaurantFoundResponse = new RestaurantList();
        restaurantFoundResponse.setRestaurantName(restaurantEntities.getRestaurant_name());
        restaurantFoundResponse.setId(UUID.fromString(restaurantEntities.getUuid()));
        restaurantFoundResponse.setPhotoURL(restaurantEntities.getPhoto_url());
        restaurantFoundResponse.setCustomerRating(restaurantEntities.getCustomer_rating());
        restaurantFoundResponse.setAveragePrice(restaurantEntities.getAverage_price_for_two());
        restaurantFoundResponse.setNumberCustomersRated(restaurantEntities.getNumber_of_customers_rated());
        restaurantFoundResponse.categories(categories);
        restaurantFoundResponse.address(new RestaurantDetailsResponseAddress()
                .id(UUID.fromString(restaurantEntities.getAddress().getUuid()))
                .flatBuildingName(restaurantEntities.getAddress().getFlat_buil_number())
                .locality(restaurantEntities.getAddress().getLocality())
                .city(restaurantEntities.getAddress().getCity())
                .pincode(restaurantEntities.getAddress().getPincode())
                .state(new RestaurantDetailsResponseAddressState()
                        .id(UUID.fromString(restaurantEntities.getAddress().getState().getStateUuid()))
                        .stateName(restaurantEntities.getAddress().getState().getStateName())
                )
        );
        restaurantDetailsResponseList.add(restaurantFoundResponse);
        RestaurantListResponse restaurantListResponse = new RestaurantListResponse().restaurants(restaurantDetailsResponseList);
        return new ResponseEntity<RestaurantListResponse>(restaurantListResponse, HttpStatus.OK);
    }

    /**
     * GET method for handling get restaurant based on category id request
     * @param category_id
     * @return restaurantListResponse
     */
    @CrossOrigin
    @RequestMapping(method = RequestMethod.GET, path = "/restaurant/category/{category_id}",produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public ResponseEntity<RestaurantListResponse> getRestaurantsByCategoryId(@PathVariable final String category_id) throws CategoryNotFoundException {
        CategoryEntity categoryEntity = categoryService.getCategoryById(category_id);
        List<RestaurantEntity> restaurantEntities = categoryEntity.getRestaurant();
        RestaurantListResponse restaurantListResponse = new RestaurantListResponse().restaurants(fetchDetails(restaurantEntities));
        return new ResponseEntity<RestaurantListResponse>(restaurantListResponse, HttpStatus.OK);
    }
    public List<RestaurantList> fetchDetails(List<RestaurantEntity> detailList) throws CategoryNotFoundException {
        List<RestaurantList> restaurantLists = new LinkedList<>();
        for (RestaurantEntity q : detailList) {
            List<CategoryEntity> categoryEntities = categoryService.getCategoriesByRestaurant(q.getUuid());
            String categories = new String();
            //To concat the category names.
            ListIterator<CategoryEntity> listIterator = categoryEntities.listIterator();
            while (listIterator.hasNext()) {
                categories = categories + listIterator.next().getCategoryName();
                if (listIterator.hasNext()) {
                    categories = categories + ", ";
                }
            }
            //Creating the RestaurantDetailsResponseAddressState for the RestaurantDetailsResponseAddress
            RestaurantDetailsResponseAddressState restaurantDetailsResponseAddressState = new RestaurantDetailsResponseAddressState()
                    .id(UUID.fromString(q.getAddress().getState().getStateUuid()))
                    .stateName(q.getAddress().getState().getStateName());

            //Creating the RestaurantDetailsResponseAddress for the RestaurantList
            RestaurantDetailsResponseAddress restaurantDetailsResponseAddress = new RestaurantDetailsResponseAddress()
                    .id(UUID.fromString(q.getAddress().getUuid()))
                    .city(q.getAddress().getCity())
                    .flatBuildingName(q.getAddress().getFlat_buil_number())
                    .locality(q.getAddress().getLocality())
                    .pincode(q.getAddress().getPincode())
                    .state(restaurantDetailsResponseAddressState);

            //Creating RestaurantList to add to list of RestaurantList
            RestaurantList restaurantList = new RestaurantList()
                    .id(UUID.fromString(q.getUuid()))
                    .restaurantName(q.getRestaurant_name())
                    .averagePrice(q.getAverage_price_for_two())
                    .categories(categories)
                    .customerRating(q.getCustomer_rating())
                    .numberCustomersRated(q.getNumber_of_customers_rated())
                    .photoURL(q.getPhoto_url())
                    .address(restaurantDetailsResponseAddress);

            //Adding it to the list
            restaurantLists.add(restaurantList);


        }
        return restaurantLists;
    }
}
