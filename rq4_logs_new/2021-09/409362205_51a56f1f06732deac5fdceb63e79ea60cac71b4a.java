package co.edu.unicauca.commandrestaurant.access;

import co.edu.unicauca.commandrestaurant.domain.Food;
import co.edu.unicauca.commandrestaurant.domain.FoodTypeEnum;
import java.util.ArrayList;
import java.util.List;

/**
 * Implementación del repositorio con arreglos
 *
 * @author Libardo, Julio
 */
public class FoodImplArrayRepository implements IFoodRepository {

    private static List<Food> components;

    public FoodImplArrayRepository() {
        if (components == null) {
            components = new ArrayList<>();
            initData();
        }
    }

    private void initData() {
        components.add(new Food(1, "Fríjoles", FoodTypeEnum.PRINCIPIO));
        components.add(new Food(2, "Sopa de verduras", FoodTypeEnum.ENTRADA));
        components.add(new Food(3, "Jugo de mango", FoodTypeEnum.JUGO));

    }

    @Override
    public Food findById(int id) {
        for (Food comp : components) {
            if (comp.getId() == id) {
                return comp;
            }
        }
        return null;
    }

    @Override
    public boolean create(Food component) {
        components.add(component);
        return true;
    }

    @Override
    public boolean update(Food component) {
        for (Food componentAux : components) {
            if (componentAux.getId() == component.getId()) {
                //Modificamos sus datos
                componentAux.setName(component.getName());
                componentAux.setType(component.getType());
                return true;
            }
        }
        return false;

    }

    @Override
    public void delete(int id) {
        int i = 0;
        for (Food componentAux : components) {
            if (componentAux.getId() == id) {
                components.remove(i);
                return;
            }
            i++;
        }
    }

    /**
     * Busca los n primeros registros de clientes
     *
     * @return lista de clientes
     */
    @Override
    public List<Food> findAll() {
        return components;
    }

}