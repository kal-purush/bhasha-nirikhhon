package org.flowershop.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.flowershop.domain.flowerShop.FlowerShop;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class FlowerShopRepositoryTXT implements IFlowerShopRepository {
    // The path to save the file.
    public static String filePath;
    File file;
    private ObjectMapper objectMapper;
    private List<FlowerShop> flowerShops;


    public FlowerShopRepositoryTXT(String fileProduct) {
        filePath = fileProduct;

        objectMapper = new ObjectMapper();
        flowerShops = new ArrayList<FlowerShop>();
        file = new File(filePath);

        loadFlowerShops();
    }

    /**
     * Add new flower shop.
     *
     * @param flowerShop  The flower shop to save
     */
    @Override
    public void addFlowerShop(FlowerShop flowerShop) {
        flowerShops.add(flowerShop);
        saveFlowerShops();
    }

    /**
     * This method returns the list of flower shops.
     *
     * @return  The list of flower shops.
     */
    @Override
    public List<FlowerShop> getAllFlowerShops() {
        return flowerShops;
    }

    /**
     * This method get a flower shop by id
     *
     * @param id
     * @return    The flower shop with the specified id
     */
    @Override
    public FlowerShop getFlowerShopById(Long id) {
        for (FlowerShop flowerShop: flowerShops) {
            if (flowerShop.getId() == id) {
                return flowerShop;
            }
        }
        return null;
    }

    /**
     * This method get a flower shop by ref.
     *
     * @param ref  The code reference of the flower shop.
     * @return     The flower shop with the specified ref.
     */
    @Override
    public FlowerShop getFlowerShopByRef(String ref) {
        for (FlowerShop flowerShop: flowerShops) {
            if (flowerShop.getRef().equals(ref)) {
                return flowerShop;
            }
        }
        return null;
    }

    /**
     * This method get a flower shop by name.
     *
     * @param name  The name of the flower shop.
     * @return      The flower shop with the specified name.
     */
    @Override
    public FlowerShop getFlowerShopByName(String name) {
        for (FlowerShop flowerShop: flowerShops) {
            if (flowerShop.getName().equals(name)) {
                return flowerShop;
            }
        }
        return null;
    }

    /**
     * This method updates the flower shop passed by parameter. The method takes the value of the
     * flower shop id and updates it.
     *
     * @param flowerShop  The flower shop with updated values.
     */
    @Override
    public void updateFlowerShop(FlowerShop flowerShop, String ref, String name) {
        if (flowerShop == null) return;

        flowerShop.setRef(ref);
        flowerShop.setName(name);

        saveFlowerShops();
    }

    /**
     * This method removes from de list the specified flower shop by id.
     *
     * @param id
     */
    @Override
    public void removeFlowerShopById(long id) {
        Iterator<FlowerShop> iterator = flowerShops.iterator();
        while (iterator.hasNext()) {
            FlowerShop flowerShop = iterator.next();
            if (flowerShop.getId() == id) {
                iterator.remove();
                break;
            }
        }
        saveFlowerShops();
    }

    /**
     * This method removes from de list the specified flower shop by id.
     *
     * @param ref
     */
    @Override
    public void removeFlowerShopByRef(String ref) {
        Iterator<FlowerShop> iterator = flowerShops.iterator();
        while (iterator.hasNext()) {
            FlowerShop flowerShop = iterator.next();
            if (flowerShop.getRef().equals(ref)) {
                iterator.remove();
                break;
            }
        }
        saveFlowerShops();
    }

    /**
     * This method saves the list of flower shops to a file.
     */
    public void saveFlowerShops() {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            for (FlowerShop flowerShop : flowerShops) {
                String json = objectMapper.writeValueAsString(flowerShop);
                writer.write(json);
                writer.newLine();
            }
        } catch (IOException e) {
            System.out.println("Exception to save flower shop to file." + e.getMessage());
        }
    }

    /**
     * This method loads the flower shops file.
     */
    public void loadFlowerShops(){
        if (!file.exists()) return;

        String line;
        FlowerShop flowerShop = null;
        List<FlowerShop> flowerShops = new ArrayList<FlowerShop>();

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            while ((line = br.readLine()) != null) {
                flowerShop = objectMapper.readValue(line, FlowerShop.class);
                flowerShops.add(flowerShop);
            }

        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.flowerShops = flowerShops;
    }

}