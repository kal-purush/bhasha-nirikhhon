package net.hiperdino.utilidades;

import net.hiperdino.productos.Alimento;
import net.hiperdino.productos.Higiene;
import net.hiperdino.productos.Hogar;
import net.hiperdino.productos.Producto;
import net.hiperdino.utilidades.TipoProducto.Tipo;

public class Generador {

  final private static String[] LISTA_NOMBRES = {
    "Ana",
    "Juan",
    "María",
    "Carlos",
    "Laura",
    "David",
    "Sandra",
    "José",
    "Patricia",
    "Francisco",
    "Cristina",
    "Ramón",
    "Isabel",
    "Luis",
    "Andrea",
    "Pedro",
    "Marta",
    "Antonio",
    "Clara",
    "Alejandro",
    "Eva",
    "Diego",
    "Raquel",
    "Pablo",
    "Vanessa",
    "Jesús",
    "Adrián",
    "Sonia",
    "Miguel",
    "Elena"
  };

  final private static Producto[] LISTA_PRODUCTOS = {
    new Higiene("Cepillo Cepillin", 2.2f, false, "1 mes"),
    new Higiene("Champu Cebolla Roja", 17.99f, true, "¡Depende de lo que se duche, guarro!"),
    new Higiene("Pasta de Dientes Nalotil", 3.99f, false, "2 semanas"),
    new Higiene("Papel Dodot Active", 77.77f, true, "Depende de lo que se cuide el usuario, aprox 2-4 semanas"),
    new Higiene("Toallas Woaw Paper ", 100f, true, "Depende del nivel fashion del usuario"),
    new Hogar("Detergete Rosame Lano", 10f, "Blanco", 1000f),
    new Hogar("Fragancia Burbujeante", 5.99f, "Amarillo", 2.5f),
    new Hogar("Esponja", 5.99f, "verde", 0.1f),
    new Hogar("Bolsas de basura", 3.49f, "negro", 0.5f),
    new Hogar("Limpiador multiusos", 2.99f, "amarillo", 0.75f),
    new Alimento("Leche", 1.99f, false, Tipo.LACTEO),
    new Alimento("Pan", 0.99f, false, Tipo.PROCESADO),
    new Alimento("Huevos", 2.49f, false, Tipo.FRESCO),
    new Alimento("Fruta", 3.99f, false, Tipo.FRESCO),
    new Alimento("Verdura", 2.29f, false, Tipo.CONGELADO),
    new Alimento("Carne", 8.99f, false, Tipo.PROCESADO),
    new Alimento("Pescado", 10.49f, false, Tipo.FRESCO),
    new Alimento("Arroz", 1.49f, false, Tipo.PREPARADO),
    new Alimento("Pasta", 1.19f, false, Tipo.PROCESADO),
    new Alimento("Legumbres", 0.89f, false, Tipo.PREPARADO)
  };

  public static int tamañoCestaAleatorio(){
    return (int) (Math.random() * 12) + 1; // Se castea a int, ya que mathrandom devuelve un float
  }

  public static String nombreAleatorio(){
    return LISTA_NOMBRES[(int) (Math.random() * 30)];
  }

  public static Producto productoAleatorio(){
    return LISTA_PRODUCTOS[(int) (Math.random() * 20)];
  }
}