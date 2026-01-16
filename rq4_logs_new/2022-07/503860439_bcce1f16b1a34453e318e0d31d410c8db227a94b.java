package LogicaNegocio;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class DatosPrenda extends Menu{
    
    String valor;
    String tipo;
    String cantidad;
   
    Integer opcion3;
    
    private List<String> listas=new ArrayList<String>();
    private Scanner teclado=new Scanner(System.in);
    private boolean bandera=false;
    
    InputStreamReader isr=new InputStreamReader(System.in);

    BufferedReader br=new BufferedReader(isr);

    public DatosPrenda() {
    }
    
    public DatosPrenda(String vvalor, String vtipo, String vcantidad){
        valor=vvalor;
        tipo=vtipo;
        cantidad=vcantidad;
    }

    public String getvalor() {
        return valor;
    }

    public void setSofás(String valor) {
        this.valor = valor;
    }

    public String gettipo() {
        return tipo;
    }

    public void settipo(String tipo) {
        this.tipo = tipo;
    }

    public String getcantidad() {
        return cantidad;
    }

    public void setcantidad(String cantidad) {
        this.cantidad = cantidad;
    }
    
    
    public void menuDatosPrenda()
    {
    do{
        try{
            System.out.println("    MENU DE DATOS DE PRENDA  ");
            System.out.println("1. Ingresar Datos");
            System.out.println("2. Imprimir Datos");
            System.out.println("3. Eliminar Datos");
            System.out.println("4. Modificar Datos");
            System.out.println("5. Buscar Datos");
            System.out.println("6. Regresar al menu principal");


            opcion3= Integer.parseInt(br.readLine());

            }catch(Exception ex)
                {
                    System.out.println("*ERROR*" + ex);
                }


    switch (opcion3)

    {
        case 1:datosSala();break;
        case 2:imprimirSala();break;
        case 3:eliminarDatos();break; 
        case 4:modificarDatos();break;
        case 5:buscarDatos(valor);break;
        case 6: menuP();break;
        default: System.out.println("Opcion no valida");
            
    }
    }while(opcion3!=4);
    }

    public void datosSala()
    {
        try{
            System.out.println("EL VALOR DE LA PRENDA ES: ");
            valor=br.readLine();
            System.out.println("EL TIPO DE LA PRENDA ES: ");
            tipo=br.readLine();
            System.out.println("LA CANTIDAD DE LA PRENDA ES: ");
            cantidad=br.readLine();

        }catch(Exception ex)
        {
            System.out.println("*ERROR*" + ex);
        }
    }

     public void imprimirSala()
    {
        try{
            System.out.println("VALOR     : "   + valor);
            System.out.println("TIPO : "        + tipo);
            System.out.println("CANTIDAD: "   + cantidad);
        }catch(Exception ex)
        {
            System.out.println("*ERROR*" + ex);
        }
    }
     
      public void modificarDatos (){
        this.valor=valor;
        this.tipo=tipo;
        this.cantidad=cantidad;
}
      
      public boolean buscarDatos(String buscado)
      {
          for (String cadena: listas) 
        {
            if(cadena.equals(buscado)){
                System.out.print("  SI ");   
                bandera=true;
            }
            else
            {
                System.out.print("  NO  ");   
            }
             System.out.println(cadena + " Es igual " + buscado + " Resultado = " + bandera);
            
        }
          return bandera;
          
      }
    

     public void eliminarDatos(){
        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter("datos.txt"));
            bw.write("");
            System.out.println("SE LIMPIARON LOS DATOS");
            bw.close();
        } catch (Exception e) {
        }
    }
}
