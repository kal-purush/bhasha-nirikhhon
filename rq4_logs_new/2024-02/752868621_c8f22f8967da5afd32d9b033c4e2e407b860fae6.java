package uniandes.dpoo.estructuras.logica;
import java.util.Arrays;
import java.util.stream.Collectors;

import java.util.HashMap;

/**
 * Esta clase tiene un conjunto de métodos para practicar operaciones sobre arreglos de enteros y de cadenas.
 *
 * Todos los métodos deben operar sobre los atributos arregloEnteros y arregloCadenas.
 * 
 * No pueden agregarse nuevos atributos.
 * 
 * Implemente los métodos usando operaciones sobre arreglos (ie., no haga cosas como construir listas para evitar la manipulación de arreglos).
 */
public class SandboxArreglos
{
    /**
     * Un arreglo de enteros para realizar varias de las siguientes operaciones.
     * 
     * Ninguna posición del arreglo puede estar vacía en ningún momento.
     */
    private int[] arregloEnteros;

    /**
     * Un arreglo de cadenas para realizar varias de las siguientes operaciones
     * 
     * Ninguna posición del arreglo puede estar vacía en ningún momento.
     */
    private String[] arregloCadenas;

    /**
     * Crea una nueva instancia de la clase con los dos arreglos inicializados pero vacíos (tamaño 0)
     */
    public SandboxArreglos( )
    {
        arregloEnteros = new int[]{};
        arregloCadenas = new String[]{};
    }

    /**
     * Retorna una copia del arreglo de enteros, es decir un nuevo arreglo del mismo tamaño que contiene copias de los valores del arreglo original
     * @return Una copia del arreglo de enteros
     */
    public int[] getCopiaEnteros( )
    {
    	int[]  CopiaEnteros = arregloEnteros.clone();
    
        return CopiaEnteros;
        
    }

    /**
     * Retorna una copia del arreglo de cadenas, es decir un nuevo arreglo del mismo tamaño que contiene copias de los valores del arreglo original
     * @return Una copia del arreglo de cadenas
     */
    public String[] getCopiaCadenas( )
    	
    {
    	String[] CopiaCadenas = arregloCadenas.clone();
        return CopiaCadenas;
    }

    /**
     * Retorna la cantidad de valores en el arreglo de enteros
     * @return
     */
    public int getCantidadEnteros( )
    {
    	int CantidadEnteros = arregloEnteros.length;
    	
        return CantidadEnteros;
        
    }

    /**
     * Retorna la cantidad de valores en el arreglo de cadenas
     * @return
     */
    public int getCantidadCadenas( )
    {
    	int CantidadCadenas = arregloCadenas.length;
        return CantidadCadenas;
        
    }

    /**
     * Agrega un nuevo valor al final del arreglo. Es decir que este método siempre debería aumentar en 1 la capacidad del arreglo.
     * 
     * @param entero El valor que se va a agregar.
     */
    public void agregarEntero( int Entero )
    {
    	int nTamanio = arregloEnteros.length +1 ;
    	int[] nArreglo = new int[nTamanio];
    	for (int i =0;i<arregloEnteros.length; i++) {
    		nArreglo[i] = arregloEnteros[i];
    		
    	}
    	nArreglo[nTamanio -1]= Entero;
    	arregloEnteros = nArreglo;
    	

    }

    /**
     * Agrega un nuevo valor al final del arreglo. Es decir que este método siempre debería aumentar en 1 la capacidad del arreglo.
     * 
     * @param cadena La cadena que se va a agregar.
     */
    public void agregarCadena( String cadena )
    {
    	int nTamanio1 = arregloCadenas.length +1;
    	String [] nArreglo = new String[nTamanio1];
    	for (int i = 0 ;i<arregloCadenas.length; i++) {
    		nArreglo[i]= arregloCadenas[i];
    	}

    	nArreglo[nTamanio1 -1] = cadena;
    	arregloCadenas = nArreglo;
    }

    /**
     * Elimina todas las apariciones de un determinado valor dentro del arreglo de enteros
     * @param valor El valor que se va eliminar
     */
    public void eliminarEntero(int valor) {
        arregloEnteros = Arrays.stream(arregloEnteros)
                               .filter(elemento -> elemento != valor)
                               .toArray();
    }

    	
    	
    
    /**
     * Elimina todas las apariciones de un determinado valor dentro del arreglo de cadenas
     * @param cadena La cadena que se va eliminar
     */
    public void eliminarCadena(String cadena) {
        arregloCadenas = Arrays.stream(arregloCadenas)
                               .filter(elemento -> !elemento.equals(cadena))
                               .toArray(String[]::new);
    }

    /**
     * Inserta un nuevo entero en el arreglo de enteros.
     * 
     * @param entero El nuevo valor que debe agregarse
     * @param posicion La posición donde debe quedar el nuevo valor en el arreglo aumentado. Si la posición es menor a 0, se inserta el valor en la primera posición. Si la
     *        posición es mayor que el tamaño del arreglo, se inserta el valor en la última posición.
     */
    public void insertarEntero(int entero, int posicion) {
        // Ajusta la posición si es menor a 0 o mayor que el tamaño del arreglo
        posicion = Math.max(0, Math.min(posicion, arregloEnteros.length));

        arregloEnteros = Arrays.copyOf(arregloEnteros, arregloEnteros.length + 1);

        System.arraycopy(arregloEnteros, posicion, arregloEnteros, posicion + 1, arregloEnteros.length - posicion - 1);

        arregloEnteros[posicion] = entero;
    }

    /**
     * Elimina un valor del arreglo de enteros dada su posición.
     * @param posicion La posición donde está el elemento que debe ser eliminado. Si el parámetro posicion no corresponde a ninguna posición del arreglo de enteros, el método
     *        no debe hacer nada.
     */
    public void eliminarEnteroPorPosicion(int posicion) {
        if (posicion < 0 || posicion >= arregloEnteros.length) {
            return;
        }

        arregloEnteros = Arrays.copyOfRange(arregloEnteros, 0, arregloEnteros.length - 1);
        System.arraycopy(arregloEnteros, posicion + 1, arregloEnteros, posicion, arregloEnteros.length - posicion - 1);
    }
    /**
     * Reinicia el arreglo de enteros con los valores contenidos en el arreglo del parámetro 'valores' truncados.
     * 
     * Es decir que si el valor fuera 3.67, en el nuevo arreglo de enteros debería quedar el entero 3.
     * @param valores Un arreglo de valores decimales.
     */
    public void reiniciarArregloEnteros(double[] valores) {
        int[] nuevoArreglo = new int[valores.length];

        for (int i = 0; i < valores.length; i++) {
            int numeroRedondeado = (int) Math.round(valores[i]);
            nuevoArreglo[i] = numeroRedondeado;
        }

        arregloEnteros = nuevoArreglo;
    }


    /**
     * Reinicia el arreglo de cadenas con las representaciones como Strings de los objetos contenidos en el arreglo del parámetro 'objetos'.
     * 
     * Use el método toString para convertir los objetos a cadenas.
     * @param valores Un arreglo de objetos
     */
    public void reiniciarArregloCadenas( Object[] objetos )
    {
    	int nuevoTamanio = objetos.length;
    	String[]nuevoArreglo = new String[nuevoTamanio];
    	for (int i = 0; i<objetos.length;i++) {
    		nuevoArreglo[i] = objetos[i].toString();
    	}
    	arregloCadenas = nuevoArreglo;
    }

    /**
     * Modifica el arreglo de enteros para que todos los valores sean positivos.
     * 
     * Es decir que si en una posición había un valor negativo, después de ejecutar el método debe quedar el mismo valor muliplicado por -1.
     */
    public void volverPositivos() {
        for (int i = 0; i < arregloEnteros.length; i++) {
            if (arregloEnteros[i] < 0) {
                arregloEnteros[i] = arregloEnteros[i] * (-1);
            }
        }
    }


    /**
     * Modifica el arreglo de enteros para que todos los valores queden organizados de menor a mayor.
     */
    public void organizarEnteros( )
    {
    	Arrays.sort(arregloEnteros);
    }

    /**
     * Modifica el arreglo de cadenas para que todos los valores queden organizados lexicográficamente.
     */
    public void organizarCadenas( )
    {
    	Arrays.sort(arregloCadenas);
    }

    /**
     * Cuenta cuántas veces aparece el valor recibido por parámetro en el arreglo de enteros
     * @param valor El valor buscado
     * @return La cantidad de veces que aparece el valor
     */
    public int contarAparicionesValue(int valor) {
        int contador = 0;
        for (int i = 0; i < arregloEnteros.length; i++) {
            if (arregloEnteros[i] == valor) {
                contador++;
            }
        }
        return contador;
    }

    /**
     * Cuenta cuántas veces aparece la cadena recibida por parámetro en el arreglo de cadenas.
     * 
     * La búsqueda no debe diferenciar entre mayúsculas y minúsculas.
     * @param cadena La cadena buscada
     * @return La cantidad de veces que aparece la cadena
     */
    public int contarAparicionescadena(String cadena) {
        int contador = 0;

        for (int i = 0; i < arregloCadenas.length; i++) {
            if (arregloCadenas[i] != null && arregloCadenas[i].equalsIgnoreCase(cadena)) {
                contador++;
            }
        }

        return contador;
    }

    /**
     * Busca en qué posiciones del arreglo de enteros se encuentra el valor que se recibe en el parámetro
     * @param valor El valor que se debe buscar
     * @return Un arreglo con los números de las posiciones del arreglo de enteros en las que se encuentra el valor buscado. Si el valor no se encuentra, el arreglo retornado
     *         es de tamaño 0.
     */
    public int[] buscarEntero(int valor) {
        int numeroApariciones = 0;

        // Contar el número de apariciones
        for (int i = 0; i < arregloEnteros.length; i++) {
            if (arregloEnteros[i] == valor) {
                numeroApariciones++;
            }
        }

        // Crear el nuevo arreglo con las posiciones
        int[] nuevoArreglo = new int[numeroApariciones];

        // Llenar el nuevo arreglo con las posiciones
        int indicePosiciones = 0;
        for (int i = 0; i < arregloEnteros.length; i++) {
            if (arregloEnteros[i] == valor) {
                nuevoArreglo[indicePosiciones] = i;
                indicePosiciones++;
            }
        }

        return nuevoArreglo;
    }

    /**
     * Calcula cuál es el rango de los enteros (el valor mínimo y el máximo).
     * @return Un arreglo con dos posiciones: en la primera posición, debe estar el valor mínimo en el arreglo de enteros; en la segunda posición, debe estar el valor máximo
     *         en el arreglo de enteros. Si el arreglo está vacío, debe retornar un arreglo vacío.
     */
    public int[] calcularRangoEnteros() {
        if (arregloEnteros == null || arregloEnteros.length == 0) {
            int[] arregloVacio = new int[0];
            return arregloVacio;
        }

        int minimo = arregloEnteros[0];
        int maximo = arregloEnteros[0];

        // Encontrar el mínimo y máximo
        for (int i = 1; i < arregloEnteros.length; i++) {
            if (minimo > arregloEnteros[i]) {
                minimo = arregloEnteros[i];
            }
            if (maximo < arregloEnteros[i]) {
                maximo = arregloEnteros[i];
            }
        }

        // Crear un nuevo arreglo con el rango
        int[] nuevoArreglo = {minimo, maximo};
        return nuevoArreglo;
    }


    /**
     * Calcula un histograma de los valores del arreglo de enteros y lo devuelve como un mapa donde las llaves son los valores del arreglo y los valores son la cantidad de
     * veces que aparece cada uno en el arreglo de enteros.
     * @return Un mapa con el histograma de valores.
     */
    public HashMap<Integer, Integer> calcularHistograma( )
    {
       HashMap<Integer, Integer>nHashMap = new HashMap<>();
       for (int i = 0;i<arregloEnteros.length;i++) {
    	   nHashMap.put(arregloEnteros[i], contarAparicionesValue(arregloEnteros[i]));
    	
       }
       return nHashMap;
    }

    /**
     * Cuenta cuántos valores dentro del arreglo de enteros están repetidos.
     * @return La cantidad de enteos diferentes que aparecen más de una vez
     */
    public int contarEnterosRepetidos() {
        if (arregloEnteros == null || arregloEnteros.length == 0) {
            return 0;  // No hay elementos para contar
        }

        int contadorRepetidos = 0;

        // Utilizamos un arreglo para marcar si un número ha sido contado
        boolean[] contado = new boolean[arregloEnteros.length];

        // Contamos los números repetidos
        for (int i = 0; i < arregloEnteros.length - 1; i++) {
            if (!contado[i]) {
                for (int j = i + 1; j < arregloEnteros.length; j++) {
                    if (!contado[j] && arregloEnteros[i] == arregloEnteros[j]) {
                        contadorRepetidos++;
                        contado[j] = true;  // Marcar como contado para evitar contar repetidos múltiples veces
                    }
                }
                contado[i] = true;  // Marcar como contado para evitar contar repetidos múltiples veces
            }
        }

        return contadorRepetidos;
    }


    /**
     * Compara el arreglo de enteros con otro arreglo de enteros y verifica si son iguales, es decir que contienen los mismos elementos exactamente en el mismo orden.
     * @param otroArreglo El arreglo de enteros con el que se debe comparar
     * @return True si los arreglos son idénticos y false de lo contrario
     */
    public boolean compararArregloEnteros(int[] otroArreglo) {
        if (arregloEnteros.length != otroArreglo.length) {
            return false;
        }

        for (int i = 0; i < arregloEnteros.length; i++) {

            if (arregloEnteros[i] != otroArreglo[i]) {

                return false;
            }
        }


        return true;
    }
    /**
     * Compara el arreglo de enteros con otro arreglo de enteros y verifica que tengan los mismos elementos, aunque podría ser en otro orden.
     * @param otroArreglo El arreglo de enteros con el que se debe comparar
     * @return True si los elementos en los dos arreglos son los mismos
     */
    public boolean mismosEnteros(int[] otroArreglo) {
        // Verifica si los arreglos tienen la misma longitud
        if (arregloEnteros.length != otroArreglo.length) {
            return false;
        }

        // Utiliza un arreglo de booleanos para marcar los elementos coincidentes
        boolean[] coincidentes = new boolean[arregloEnteros.length];

        // Itera sobre cada elemento en arregloEnteros
        for (int i = 0; i < arregloEnteros.length; i++) {
            boolean encontrado = false;

            // Itera sobre cada elemento en otroArreglo
            for (int j = 0; j < otroArreglo.length; j++) {
                // Compara el elemento actual de arregloEnteros con cada elemento en otroArreglo
                if (!coincidentes[j] && arregloEnteros[i] == otroArreglo[j]) {
                    encontrado = true;
                    coincidentes[j] = true;  // Marcar como coincidente
                    break;  // Sale del bucle interno si encuentra una coincidencia
                }
            }

            // Si no se encontró una coincidencia para el elemento actual de arregloEnteros, retorna false
            if (!encontrado) {
                return false;
            }
        }

        // Si ha llegado hasta aquí, significa que todos los elementos son iguales
        return true;
    }


    /**
     * Cambia los elementos del arreglo de enteros por una nueva serie de valores generada de forma aleatoria.
     * 
     * Para generar los valores se debe partir de una distribución uniforme usando Math.random().
     * 
     * Los números en el arreglo deben quedar entre el valor mínimo y el máximo.
     * @param cantidad La cantidad de elementos que debe haber en el arreglo
     * @param minimo El valor mínimo para los números generados
     * @param maximo El valor máximo para los números generados
     */
    public void generarEnteros(int cantidad, int minimo, int maximo) {
        arregloEnteros = new int[cantidad];
        
        for (int i = 0; i < cantidad; i++) {
            int Aleatorio = (int) (Math.random() * (maximo - minimo + 1)) + minimo;
            arregloEnteros[i] = Aleatorio;
        }
    }
}