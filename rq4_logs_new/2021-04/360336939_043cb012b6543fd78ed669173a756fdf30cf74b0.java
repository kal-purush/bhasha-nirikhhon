
package co.unicauca.restaurant.DishApp;

import co.unicauca.restaurant.domain.Dish;
import co.unicauca.restaurant.domain.Size;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

/**
 *
 * @author Eliana Andrea Araujo Camayo  cod: 104616021725
 * @author Jesus Edwin Adrada Ruiz  cod: 104617020514
 * @author Carlos Enrique Hoyos Joiro  cod: 104617020768
 */
public class ItalianDish extends Dish
{
    private String italianName;
    
    /**
     *
     */
    public ItalianDish() {
        this.partes = new ArrayList<Dish>();
    }

    /**
     *
     * @param italianName
     */
    public ItalianDish(String italianName) {
        this.italianName = italianName;
    }

    /**
     *
     * @param italianName
     * @param name
     * @param despcripcion
     * @param imagen
     * @param price
     * @param partes
     * @param size
     */
    public ItalianDish(String italianName, String name, String despcripcion, String imagen, double price, List<Dish> partes, Size size) {
        super(name, despcripcion, imagen, price, partes, size);
        this.italianName = italianName;
    }

    /**
     *
     * @return
     */
    public String getItalianName() {
        return italianName;
    }

    /**
     *
     * @param italianName
     */
    public void setItalianName(String italianName) {
        this.italianName = italianName;
    }

    /**
     *
     * @return
     */
    public String getName() {
        return name;
    }

    /**
     *
     * @param name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     *
     * @return
     */
    public String getDespcripcion() {
        return despcripcion;
    }

    /**
     *
     * @param despcripcion
     */
    public void setDespcripcion(String despcripcion) {
        this.despcripcion = despcripcion;
    }

    /**
     *
     * @return
     */
    public String getImagen() {
        return imagen;
    }

    /**
     *
     * @param imagen
     */
    public void setImagen(String imagen) {
        this.imagen = imagen;
    }
    
    /**
     * @return the price
     */
    public double getPrice() {
        price = this.price;
        for(Dish each: partes){
            price = price + each.getPrice();
        }
        if(size==Size.HALF) price = 0.5*price;
        if(size==Size.ALL) price = 1*price;
        return price;
    }

    /**
     * @param price the price to set
     */
    public void setPrice(double price) {
        this.price = price;
    }

    /**
     *
     * @return
     */
    public List<Dish> getPartes() {
        return partes;
    }

    /**
     *
     * @param partes
     */
    public void setPartes(List<Dish> partes) {
        this.partes = partes;
    }
    
    /**
     *
     * @param part
     */
    public void addPart(Dish part){
        this.partes.add(part);
    }

    /**
     *
     * @return
     */
    public Size getSize() {
        return size;
    }

    /**
     *
     * @param size
     */
    public void setSize(Size size) {
        this.size = size;
    }
    
}