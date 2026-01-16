package com.toad.repositories;

import java.util.List;

import org.antlr.v4.runtime.atn.SemanticContext.AND;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;

import com.toad.entities.Inventory;

public interface InventoryDisponibleRepository extends CrudRepository<Inventory, Integer> {

    
    @Query(value = "SELECT MIN(inventory_id)\r\n" + //
                "FROM inventory \r\n" + //
                "WHERE film_id = \"?\"\r\n" + //
                "AND NOT EXISTS (\r\n" + //
                "    SELECT rental.inventory_id \r\n" + //
                "    FROM rental \r\n" + //
                "    WHERE rental.inventory_id = inventory.inventory_id \r\n" + //
                "    AND rental.return_date IS NOT NULL\r\n" + //
                ");", nativeQuery = true)
    Integer findFreeInventoryIds();
    

}