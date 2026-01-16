package EntityCreation;

import EntityCreation.IngredientCreation.IngredientCreator;

import java.util.Scanner;

public class IngredientException {
    Scanner sc = new Scanner(System.in);
    private IngredientCreator IC;

    public void run(String name) {
        System.out.println("Ingredient Doesn't Exist");
        System.out.println("Input attributes to Create Ingredient: 'DONE' when finished");
        StringBuilder data = new StringBuilder();
        data.append(name + '@');
        String att = sc.nextLine();
        while (!(att.equals("DONE"))) {
            data.append(att + " ");
            att = sc.nextLine();
        }
        IC = new IngredientCreator(data.toString());
        IC.create();
    }
}


