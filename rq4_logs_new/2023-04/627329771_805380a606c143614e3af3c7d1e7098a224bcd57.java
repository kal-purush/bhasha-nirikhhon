package org.flowershop.controller;

import org.flowershop.repository.ProductRepositoryTXT;
import org.flowershop.repository.TicketRepositoryTXT;
import org.flowershop.service.ProductService;
import org.flowershop.service.TicketService;
import org.flowershop.utils.Menu;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;


public class FlowerShopController {

    private final ProductController productController;
    private final TicketController ticketController;

    public FlowerShopController() {
        this.productController = new ProductController();
        this.ticketController = new TicketController();
    }

    public void mainDataRequest() {

        Properties properties = new Properties();
        String fileTicket = "";
        String fileProduct = "";
        try {
            String directoryProgram = System.getProperty("user.dir");
            String configFile = directoryProgram + "\\src\\main\\resources\\config.properties";
            properties.load(new FileInputStream(new File(configFile)));

            fileTicket = directoryProgram + "\\" + (String) properties.get("fileTicket");
            fileProduct = directoryProgram + "\\" + (String) properties.get("fileProduct");

        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        TicketService ticketService = new TicketService(new TicketRepositoryTXT(fileTicket));
        ProductService productService = new ProductService(new ProductRepositoryTXT(fileProduct));

        Boolean exit = false;

        try {
            do {
                try {
                    int option = Menu.showMainMenu();
                    System.out.println(option);
                    switch (option) {
                        case 1:
                            //TODO Go to Products Menu
                            productController.productDataRequest(productService);
                            break;
                        case 2:
                            //TODO Go to Tickets Menu
                            ticketController.ticketDataRequest(ticketService, productService);
                            break;
                        case 3:
                            //TODO Go to Queries Menu
                            //removeProductInTicketDetail(ticketDetails);
                            break;
                        case 4:
                            //TODO Exit
                            exit = true;
                            break;

                        default:
                            System.out.println("Incorrect option");
                            break;
                    }
                } catch (Exception e) {
                    System.out.println("Incorrect option");
                }
            } while (!exit);
        } catch (Exception e) {
            System.out.println("Bye!!!");
        }


    }

}

