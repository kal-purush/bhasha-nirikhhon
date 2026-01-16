package airam.museo;

import java.time.LocalDate;


public class Controlador {

    public void menu(){
        int opcion = 0;
        Vista vista = new Vista();
	do {
            opcion = vista.elegirOpcion("Crear Obras", "Elimina Obra","Busca Obra"
                       , "Supercie pinturas", "Escultura mas alta","Mostrar obras");
            switch (opcion) 
             {
              case 1:
                  DAO.crearObras();
                  break;
              case 2:
                  vista.leerObra();
                  break;

              case 3:
                  DAO.catalogo.eliminarObra(vista.pedirNumInentario());
                  break;
              case 4:
                  DAO.catalogo.superficie();
                  break;

              case 5:
                  DAO.catalogo.masAlta();
                  break;

              case 6:
                  vista.mostrar();
                  break;
              } 	     
        }while (opcion!=6);
    }	
}