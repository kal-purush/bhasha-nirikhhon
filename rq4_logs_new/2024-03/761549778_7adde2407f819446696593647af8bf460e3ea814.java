
package tec.poo.tareas.servicio_bibliotecas;

import java.util.Scanner;

public class Biblioteca {
    public static Libros[] lista_libros = new Libros[100];
    public static Usuarios[] lista_usuarios = new Usuarios[100];

    public int disponibles = 0;
    public int prestados = 0;

    public static int total_libros = 0;
    private static int total_usuarios = 0;

    public static void main(String[] args) {
        int salir = 0;

        Scanner entrada = new Scanner(System.in); // Mover la creación del Scanner aquí

        do {

            System.out.println("Menú: ");
            int opcion = menu();

            switch (opcion) {
                case 1:
                    obtener_libro_por_titulo();
                    break; 
                case 2:
                    agregar_libros();
                    break; 
                case 3:
                    agregar_usuarios();
                    break; 
                case 4:
                    System.out.println("Indique el titulo del libro:");
                    String titulo = entrada.nextLine();
                    
                    int prestado = esta_prestado(titulo);
                    prestar_libro (prestado,titulo);
                    break;
                case 5:
                    obtener_todos_libros();
                    break;
               
                case 7:
                    obtener_usuarios();
                    break;
                case 0:
                    salir = 1;
                    break;
                default:
                    System.out.println("Opción no válida.");
            }

        } while (salir == 0);

        entrada.close(); // Cerrar Scanner al final del main
    }

    private static int menu() {

        Scanner entrada = new Scanner(System.in);
        int opcion;

        System.out.println("--------------------------------");
        System.out.println("1. Obtener libro por titulo.");
        System.out.println("2. Agregar libro.");
        System.out.println("3. Crear Usuario.");
        System.out.println("4. Prestar Libro");
        System.out.println("5. Obtener todos los libros");
        System.out.println("7. Obtener Usuarios");
        System.out.println("0. Salir de la aplicación.");
        System.out.println("--------------------------------");

        System.out.println("Indique su opción: ");
        opcion = entrada.nextInt();
        System.out.println("--------------------------------");

        return opcion;

    }

    private static void agregar_usuarios() {
        Scanner entrada = new Scanner(System.in);

        System.out.println("Indique el nombre:");
        String Nombre = entrada.nextLine();

        System.out.println("Indique la identificacion:");
        int ID = entrada.nextInt();

        System.out.println("Indique el edad:");
        int Edad = entrada.nextInt();

        Usuarios nuevo_usuario = new Usuarios(Nombre, ID, Edad);

        lista_usuarios[total_usuarios] = nuevo_usuario;
        total_usuarios++;
    }

    private static void agregar_libros() {
        Scanner entrada = new Scanner(System.in);

        System.out.println("Indique el nombre:");
        String Titulo = entrada.nextLine();

        System.out.println("Indique el Autor:");
        String Autor = entrada.nextLine();

        System.out.println("Indique el ISBN:");
        int ISBN = entrada.nextInt();

        Libros nuevoLibro = new Libros(Titulo, Autor, ISBN);

        lista_libros[total_libros] = nuevoLibro;
        total_libros++;

        System.out.println("libro agregado correctamente.");
    }

    private static void obtener_libro_por_titulo() {
        Scanner entrada = new Scanner(System.in);

        System.out.println("Indique el titulo del libro:");
        String titulo = entrada.nextLine();

        for (int i = 0; i < total_libros; i++) {
            if (lista_libros[i].Titulo.equals(titulo)) { // Usar equals para comparar Strings
                System.out.println("Libro existe!!");

                System.out.println(lista_libros[i].Titulo);
                System.out.println(lista_libros[i].Autor);
                System.out.println(lista_libros[i].ISBN);

                break;
            }
        }
    }
    
    private static void obtener_usuarios(){
        for(int i = 0; i < total_usuarios; i++){
            int j = i +1;
            System.out.println("--------------------------------");
            String nombre = lista_usuarios[i].Nombre;
            System.out.println("Usuario: " + j + ", nombre: " + nombre);
            
        }
        System.out.println("--------------------------------");
    }
    private static void obtener_todos_libros() {
        for (int i = 0; i < total_libros; i++) { // Corregir el límite del bucle
            String titulo = lista_libros[i].Titulo;
            int j = i + 1;
            System.out.println("--------------------------------");
            System.out.println("Libro: " + j + ", nombre: " + titulo);

        }
        System.out.println("--------------------------------");
    }

    
    private static int esta_prestado(String titulo){
        
        for (int i = 0; i < total_libros; i++){
            if(lista_libros[i].Titulo.equals(titulo)){ // Verifica si el libro existe
                if(lista_libros[i].Prestado == 1){ // Verifica si está prestado
                    return (1);
                } else if(lista_libros[i].Prestado == 0){ // verifica si no está prestado.
                    return (0);
                }
            }
        }
        System.out.println("Libro no existe.");
        return (2);
        
    }
    

    private static void prestar_libro(int prestado, String titulo){
        Scanner entrada = new Scanner(System.in);
        if (prestado == 1){
           System.out.println("Libro ya prestado");
        }else if (prestado == 2){
           System.out.println("Libro no existe");
        }
        
        System.out.println("Indique el ID de usuario para prestarle el libro:");
        int ID = entrada.nextInt();        
        
        int existe = existe_usuario(ID);
        if (existe == 1){
            for (int i = 0; i< total_libros; i++){
                if(lista_libros[i].Titulo.equals(titulo)){
                    lista_libros[i].Prestado = 1;
                }
            }
            System.out.println("Libro prestado!!");
            System.out.println("--------------------------------");
        }else{
            System.out.println("Libro no existe!!");
            System.out.println("--------------------------------");
        }
    }
    private static int existe_usuario(int ID){
        for (int i = 0; i < total_usuarios; i++){
            if(lista_usuarios[i].ID == ID){
                return (1);
            }
        }   
        return (0);
    }    

}