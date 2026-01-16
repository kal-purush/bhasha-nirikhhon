import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;
public class Agencia {
  public static void main (String [] args){
    String nombre;
    int auto; 
    int venta;
    String vender;
    Scanner sc = new Scanner(System.in);
      String marca;
      int clave,clave1,clave2,clave3;
      Boolean deacuerdo;
      int km;
      int año;

      System.out.println ("Bienvenido a la compañia xd");
      System.out.println ("ingrese su nombre");
            nombre = sc.next();
      System.out.println("nombre del cliente:" + nombre);
      System.out.println("genere una clave numerica para confirmar su registro");
            clave=sc.nextInt();
      System.out.println("Su clave es: "+ clave);
      System.out.println ("registro exitoso");
      System.out.println("Si desea vender escriba: 1, Si desea comprar escriba: 2");                
                        venta = sc.nextInt();
      if (venta==1){
              Cliente Usuario1=new Cliente();
        System.out.println ("Formato para cotización de la compra");
        System.out.println ("ingrese el kilometraje de su vehiculo");
        km=sc.nextInt();
        Usuario1.setKilometraje(km);
        System.out.println ("ingrese el año del vehiculo");
        año=sc.nextInt();
        Usuario1.setAño(año);
        System.out.println("su auto esta cotizado en un valor de: " + ((((((año-2021)*0.1)+((20000-km)*0.0001))*(270000))*0.01)+270000) + "pesos");
        System.out.println("esta usted de acuerdo con la oferta que le ofrece la agencia: |true| |false|");
        deacuerdo=sc.nextBoolean();
        if (deacuerdo==true) {
                System.out.println("gracias por su preferencia" + nombre);
              } 
        if (deacuerdo==false){
          System.out.println("lamentamos no poder ofrecer una oferta deacuerdo a sus intereses");
        }       
            } else{
      System.out.println("Bienvenido al menu de compra");
      System.out.println("Contamos con los siguientes automoviles: ");
      Autos Volkswagen=new Autos();
      System.out.println("De la marca Volkswagen:");
      System.out.println("Marca: " + Volkswagen.getMarca());
      System.out.println("Color: " + Volkswagen.getColor());
      System.out.println("Matricula: " + Volkswagen.getMatricula());
      System.out.println("Modelo: " + Volkswagen.getModelo());       
      System.out.println("Disponible: " + Volkswagen.getDisponible()); 

      Autos Kia=new Autos();
      System.out.println("De la marca Kia:");
      Kia.setMarca("Kia fiesta");
      System.out.println ("Marca: " + Kia.getMarca());
      Kia.setColor("Rojo");
      System.out.println("Color: "+Kia.getColor());
      Kia.setMatricula("K21r");
      System.out.println("Matricula: "+Kia.getMatricula());
      Kia.setModelo(2021);
      System.out.println("Modelo: "+Kia.getModelo());
      Kia.setDisponible(true);
      System.out.println("Disponible: "+Kia.getDisponible());

      Autos Camaro=new Autos();
      System.out.println("De la marca Chevi:");
      Camaro.setMarca("Chevrolet Camaro");
      System.out.println ("Marca " + Camaro.getMarca());
      Camaro.setColor("Amarillo");
      System.out.println("Color: "+Camaro.getColor());
      Camaro.setMatricula("CHC21a");
      System.out.println("Matricula: "+Camaro.getMatricula());
      Camaro.setModelo(2021);
      System.out.println("Modelo: "+Camaro.getModelo());
      Camaro.setDisponible(true);
      System.out.println("Disponible: "+Camaro.getDisponible());

      System.out.println("Si desea comprar el Volkswagen escriba 1, si desea comprar el Kia escriba 2, si desea comprar el Camaro escriba 3, si desea salir de la agencia, escriba otro numero");
      auto = sc.nextInt();
      if(auto==1){
        System.out.println("contamos con la siguiente promoción: ");
        Vendedor Vendedor=new Vendedor();
      System.out.println("Marca: " + Vendedor.getCosto());
      System.out.println("Color: " + Vendedor.getPlan());
      System.out.println("Matricula: " + Vendedor.getSeguro());
      System.out.println("Modelo: " + Vendedor.getDonativo());
      System.out.println ("si desea realizar la compra, ingrese su clave para continuar, en caso contrario ingrese otro numero");
      clave1 = sc.nextInt();
      if (clave1==clave) {
        System.out.println("gracias por su preferencia " + nombre);
          DateFormat formato=new SimpleDateFormat("dd/MM/YYYY");
              System.out.println("Fecha de adquisicion: "+formato.format(new Date()));
                       }
                     else{
System.out.println("la clave ingresada es incorrecta, lamentamos no contar con un producto de su agrado");
                     }               
    } 
        if (auto==2){
          System.out.println("contamos con la siguiente promoción: ");
        Vendedor Vendedor1=new Vendedor();
      System.out.println("Costo: " + Vendedor1.getCosto());
      Vendedor1.setCosto(100000);
      System.out.println("Plan de pago: " + Vendedor1.getPlan());
            Vendedor1.setPlan(24);
      System.out.println("seguro: " + Vendedor1.getSeguro());
                  Vendedor1.setSeguro(true);
      System.out.println("Donativo: " + Vendedor1.getDonativo());
      Vendedor1.setDonativo(true);
      System.out.println ("si desea realizar la compra, ingrese su clave para continuar, en caso contrario ingrese otro numero");
      clave2 = sc.nextInt();
      if (clave2==clave){
        System.out.println("gracias por su preferencia " + nombre);
      DateFormat formato=new SimpleDateFormat("dd/MM/YYYY");
      System.out.println("Fecha de adquisicion: "+formato.format(new Date()));
    }
     else{
System.out.println("la clave ingresada es incorrecta, lamentamos no contar con un producto de su agrado");
                     }   
  }
        if (auto==3){
System.out.println("contamos con la siguiente promoción: ");
        Vendedor Vendedor2=new Vendedor();
      System.out.println("Costo: " + Vendedor2.getCosto());
      Vendedor2.setCosto(100000);
      System.out.println("Plan de pago: " + Vendedor2.getPlan());
            Vendedor2.setPlan(24);
      System.out.println("seguro: " + Vendedor2.getSeguro());
                  Vendedor2.setSeguro(true);
      System.out.println("Donativo: " + Vendedor2.getDonativo());
      Vendedor2.setDonativo(true);
      System.out.println ("si desea realizar la compra, ingrese su clave para continuar, en caso contrario ingrese otro numero");
      clave3 = sc.nextInt();
      if (clave3==clave){
        System.out.println("gracias por su preferencia " + nombre);
      DateFormat formato=new SimpleDateFormat("dd/MM/YYYY");
      System.out.println("Fecha de adquisicion: "+formato.format(new Date()));
    }
     else{
System.out.println("la clave ingresada es incorrecta, lamentamos no contar con un producto de su agrado");
                     }   
  }
      
  }
}
}