import java.sql.SQLException;
import java.util.List;
import java.util.TreeSet;

public class MenuGasto {
    private Escaner escaner;
    private BaseDeDatos bd;
    private Grupo grupo;
    private Gasto gasto;
    private int opcion;

    public MenuGasto() {
        opcion = 0;
    }

    public void crearGasto() throws SQLException {
        System.out.println("Para proceder con la creación del gasto, por favor, introduzca los siguientes datos:");

        System.out.print("Título: ");
        String titulo = escaner.entradaCadena();

        System.out.print('\n' + "Descripción: ");
        String descripcion = escaner.entradaCadena();

        System.out.print('\n' + "Elija a continuación la categoría: ");
        int idCategoria = obtenerCategoria();

        System.out.print('\n' + "Coste del gasto: ");
        double coste = escaner.entradaDecimal();

        System.out.print('\n' + "Elija los participantes: ");
        TreeSet<Usuario> participantes = crearListaParticipantes();

        gasto = new Gasto(titulo, descripcion, coste, participantes, grupo.getAdmin, grupo.getIdGrupo, idCategoria);
        System.out.println("El gasto ha sido creado correctamente.");
    }

    private TreeSet<Usuario> crearListaParticipantes() throws SQLException {
        List<Usuario> listaUsuarios = bd.obtenerListaUsuarios();
        TreeSet<Usuario> conjuntoParticipantes = new TreeSet<Usuario>();
        int opcionParticipantes = -1;

        //Recorremos la lista de usuarios imprimiendo uno por uno.
        for (int i = 1; i<=listaUsuarios.size(); i++) {
            System.out.println(i + ". " + listaUsuarios.get(i-1));
        }
        System.out.println("0. Terminar");

        //Creamos un bucle para que el usuario decida los participantes del gasto.
        while (opcionParticipantes != 0) {
            opcionParticipantes = escaner.entradaEntero();
            conjuntoParticipantes.add(listaUsuarios.get(opcionParticipantes));
        }

        //Devolvemos el conjunto de los participantes
        return conjuntoParticipantes;
    }

    private int obtenerCategoria() throws SQLException {
        List<String> listaCategorias = bd.obtenerListaCategoria();
        int opcionCategoria;
        int listaSize = listaCategorias.size();

        //Recorremos la lista con las categorias imprimiendo una a una
        for (int i = 1; i<=listaCategorias.size(); i++) {
            System.out.println(i + ". " + listaCategorias.get(i-1));
        }
        System.out.println(listaSize + ". Categoría Personalizada");

        //Damos la opción al usuario de elegir la categoría
        opcionCategoria = escaner.entradaEntero();
        //Si este decide crear una nueva categoría, llamamos al método pertinente y almacenamos el id de la nueva categoría
        if (opcionCategoria == listaSize) {
            opcionCategoria = bd.agregarCategoria(categoriaPersonalizada());
        }

        //Devolvemos el id de la categoria seleccionada
        return opcionCategoria;
    }
    //Función para crear una categoría personalizada y devolver su id
    private String categoriaPersonalizada() {
        System.out.println("Introduzca a continuación el nombre de la nueva categoría:");
        return escaner.entradaCadena();
    }
}