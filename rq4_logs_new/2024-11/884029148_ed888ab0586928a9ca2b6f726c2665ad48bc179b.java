package data;

//import entity.CommonIngredient;

import entity.CommonIngredient;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class txtConnector implements Connector {
    private final String fileName = "Data.txt";
    private CommonIngredient ingredient;

    public txtConnector(CommonIngredient ingredient) {
        this.ingredient = ingredient;
    }

    @Override
    public void save() {
        try {
            FileWriter fw = new FileWriter(fileName, true);
            if (ingredient.getName().contains("|")){
                throw new IOException("name format error");
            }
            if (finder()) {
                throw new IOException("repeated name error");
            }
            fw.write(ingredient.getName()+"|"+ingredient.getExpiryDate());
            fw.write(System.lineSeparator());
            fw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void delete() {
        try {
            if (!finder()) {
                throw new IOException("ingredient not found");
            }
            List<String> lines = Files.readAllLines(Paths.get(fileName));
            List<String> updatedLines = new ArrayList<>();
            for (String line : lines) {
                if (!line.split("\\|")[0].equals(ingredient.getName())) {
                    updatedLines.add(line);
                }
            }
            Files.write(Paths.get(fileName), updatedLines);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public LocalDate searchDate() {
        try {
            List<String> lines = Files.readAllLines(Paths.get(fileName));
            for (String line : lines) {
                if (!line.split("\\|")[0].equals(ingredient.getName())) {
                    return LocalDate.parse(line.split("\\|")[1]);
                }
            }
            throw new IOException("Ingredient not found");
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private boolean finder() throws FileNotFoundException {
        File file = new File(fileName);
        Scanner scanner = new Scanner(file);
        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            String ingredientName = line.split("\\|")[0];
            System.out.println("ingredient name = " + ingredientName);
            if (ingredientName.equals(ingredient.getName())){
                return true;
            }
        }
        return false;
    }
}