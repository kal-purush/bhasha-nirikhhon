package org.flowershop.repository;

import com.fasterxml.jackson.databind.JsonNode;

import org.flowershop.domain.products.Decoration;
import org.flowershop.domain.products.Flower;
import org.flowershop.domain.products.Product;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.flowershop.domain.products.Tree;


public class ProductRepositoryTXT implements IProductRepository {
    // Put the path to save the file.
    public static String filePath;
    File file;
    private ObjectMapper objectMapper;
    private List<Product> products;


    public ProductRepositoryTXT() {
        // Put the file path to save
        filePath = "..\\flowerShopData\\products.txt";

        objectMapper = new ObjectMapper();
        products = new ArrayList<Product>();
        file = new File(filePath);

        loadProducts();
    }

    /**
     * This method saves the list of products to a file.
     */
    @Override
    public void saveProducts() {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            for (Product product : products) {
                String json = objectMapper.writeValueAsString(product);
                writer.write(json);
                writer.newLine();
            }
        } catch (IOException e) {
            System.out.println("Exception to save products to file." + e.getMessage());
        }
    }

    /**
     * This method loads the products file.
     */
    @Override
    public void loadProducts() {
        if (!file.exists()) return;

        String line;
        List<Product> products = new ArrayList<Product>();

        ObjectMapper objectMapper = new ObjectMapper();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {

            while ((line = br.readLine()) != null) {
                // Read file line as a tree of json nodes
                JsonNode jsonNode = objectMapper.readTree(line);

                // Get product type
                String productType = jsonNode.get("productType").asText();

                // Deserialize the JSON node to a correct class.
                switch (productType) {
                    case "tree":
                        Tree tree =  objectMapper.treeToValue(jsonNode, Tree.class);
                        products.add(tree);
                        break;
                    case "flower":
                        Flower flower =  objectMapper.treeToValue(jsonNode, Flower.class);
                        products.add(flower);
                        break;
                    case "decoration":
                        Decoration decoration = objectMapper.treeToValue(jsonNode, Decoration.class);
                        products.add(decoration);
                        break;
                    default:
                        System.out.println("Type class not found.");
                }
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.products = products;
    }

    /**
     * Add new product to the list.
     *
     * @param product  The product to save.
     */
    @Override
    public void addProduct(Product product) {
        products.add(product);
        saveProducts();
    }

    /**
     * This method returns the list of products.
     *
     * @return the list of products.
     */
    @Override
    public List<Product> getProducts() {
        return products;
    }

    /**
     * This method returns a product by id.
     *
     * @param id  The product id.
     * @return    The product with the specified id.
     */
    @Override
    public Product getProductById(long id) {
        for (Product product: products) {
            if (product.getId() == id) {
                return product;
            }
        }
        return null;
    }

    /**
     * This method returns a product by ref.
     *
     * @param ref  The code reference of the product.
     * @return     The product with the specified reference.
     */
    @Override
    public Product getProductByRef(String ref) {
        for (Product product: products) {
            if (product.getRef().equals(ref)) {
                return product;
            }
        }
        return null;
    }

    /**
     * This method updates the product passed by parameter. The method takes the value of the
     * product id and updates it to the new product.
     *
     * @param product  The product with the new values to update.
     */
    @Override
    public void updateProductById(long id, Product product) {
        for (int index = 0; index < products.size(); index++) {
            if (products.get(index).getId() == id) {
                products.set(index, product);
                break;
            }
        }
        saveProducts();
    }

    /**
     * This method removes from de list the specified product by id.
     *
     * @param id  The product id.
     */
    @Override
    public void removeProductById(long id) {
        Iterator<Product> iterator = products.iterator();
        while (iterator.hasNext()) {
            Product product = iterator.next();
            if (product.getId() == id) {
                iterator.remove();
                break;
            }
        }
        saveProducts();
    }

    /**
     * This method removes from de list the specified product by code reference.
     *
     * @param ref  The code reference od the product.
     */
    @Override
    public void removeProductByRef(String ref) {
        Iterator<Product> iterator = products.iterator();
        while (iterator.hasNext()) {
            Product product = iterator.next();
            if (product.getRef().equals(ref)) {
                iterator.remove();
                break;
            }
        }
        saveProducts();
    }


}