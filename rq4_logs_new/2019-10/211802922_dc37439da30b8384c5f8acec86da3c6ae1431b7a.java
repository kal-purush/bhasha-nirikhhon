package com.ezgroceries.shoppinglist.repository;

import com.ezgroceries.shoppinglist.model.CocktailDbResponse;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

@Component
@FeignClient(name = "cocktailDBClient", url = "https://www.thecocktaildb.com/api/json/v1/1")
public interface CocktailDbRemoteClient extends CocktailDbClient {

    @GetMapping(value = "search.php")
    CocktailDbResponse searchCocktails(@RequestParam("s") String search);

}