package com.qa.ims.controller;

import com.qa.ims.persistence.dao.ItemDAO;
import com.qa.ims.persistence.domain.Item;
import com.qa.ims.utils.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 *  Takes in item details for CRUD functionality
 *
 */
public class ItemController implements CrudController<Item>
{
    public static final Logger LOGGER = LogManager.getLogger();

    private ItemDAO itemDAO;
    private Utils utils;

    public ItemController(ItemDAO itemDAO, Utils utils)
    {
        super();
        this.itemDAO = itemDAO;
        this.utils = utils;
    }

    /**
     * Reads all items to the Logger
     */
    @Override
    public List<Item> readAll()
    {
        List<Item> items = itemDAO.readAll();
        for (Item item : items)
        {
            LOGGER.info(item.toString());
        }
        return items;
    }

    /**
     * Creates an item by taking in user input
     */
    @Override
    public Item create()
    {
        LOGGER.info("Please enter the item name");
        String name = utils.getString();
        LOGGER.info("Please enter the item price");
        double price = utils.getDouble();
        Item item = itemDAO.create(new Item(name, price));
        LOGGER.info("Item created");
        return item;
    }

    /**
     * Updates an existing item by taking in user input
     */
    @Override
    public Item update()
    {
        LOGGER.info("Please enter the id of the item you would like to update");
        Long id = utils.getLong();
        LOGGER.info("Please enter the new item name");
        String name = utils.getString();
        LOGGER.info("Please enter the new item price");
        double price = utils.getDouble();
        Item item = itemDAO.update(new Item(id, name, price));
        LOGGER.info("Item Updated");
        return item;
    }

    /**
     * Deletes an existing item by the id of the item
     */
    @Override
    public int delete()
    {
        LOGGER.info("Please enter the id of the item you would like to delete");
        Long id = utils.getLong();
        return itemDAO.delete(id);
    }
}