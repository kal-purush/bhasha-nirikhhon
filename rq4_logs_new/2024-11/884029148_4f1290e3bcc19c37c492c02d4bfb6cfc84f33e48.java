package data_access;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDate;
import java.util.*;

import entity.Ingredient;
import entity.IngredientFactory;
import use_case.addorcancelingredient.AddorCancelIngredientIngredientDataAccessInterface;
import use_case.delete_ingredient.DeleteIngredientIngredientDataAccessInterface;

public class FileIngredientDataAccessObject implements AddorCancelIngredientIngredientDataAccessInterface,
        DeleteIngredientIngredientDataAccessInterface {
    private static final String HEADER = "Ingredients";

    private final File csvFile;
    private final Map<String, Integer> headers = new LinkedHashMap<>();
    private final Map<String, Ingredient> ingredients = new HashMap<>();
    private List<Ingredient> currentIngredients;

    public FileIngredientDataAccessObject(String csvPath, IngredientFactory ingredientFactory) throws IOException {

        csvFile = new File(csvPath);
        headers.put("ingredient name", 0);
        headers.put("expiry date", 1);

        if (csvFile.length() == 0) {
            save();
        }
        else {

            try (BufferedReader reader = new BufferedReader(new FileReader(csvFile))) {
                final String header = reader.readLine();

                if (!header.equals(HEADER)) {
                    throw new RuntimeException(String.format("header should be%n: %s%but was:%n%s", HEADER, header));
                }

                String row;
                while ((row = reader.readLine()) != null) {
                    final String[] col = row.split(",");
                    final String ingredientName = String.valueOf(col[headers.get("ingredient name")]);
                    long expiryDateEpochDay = Long.parseLong(col[headers.get("expiry date")]);
                    LocalDate expiryDate = LocalDate.ofEpochDay(expiryDateEpochDay);
                    final Ingredient ingredient = ingredientFactory.create(ingredientName, expiryDate);
                    ingredients.put(ingredientName, ingredient);
                    currentIngredients.add(ingredient);
                }
            }
        }
    }

    private void save() {
        final BufferedWriter writer;
        try {
            writer = new BufferedWriter(new FileWriter(csvFile));
            writer.write(String.join(",", headers.keySet()));
            writer.newLine();

            for (Ingredient ingredient : ingredients.values()) {
                final String line = String.format("%s,%s",
                        ingredient.getName(), ingredient.getExpiryDate().toString());
                writer.write(line);
                writer.newLine();
            }

            writer.close();

        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public boolean existsByIngredientName(String ingredientName) {
        return ingredients.containsKey(ingredientName);
    }

    @Override
    public void save(Ingredient ingredient) {
        ingredients.put(ingredient.getName(), ingredient);
    }

    @Override
    public List<Ingredient> getAllIngredients() {
        return currentIngredients;
    }

    @Override
    public List<Ingredient> getCurrentIngredients() {
        return currentIngredients;
    }

    @Override
    public void deleteIngredient(Ingredient ingredient) {
        ingredients.remove(ingredient.getName());
    }
}