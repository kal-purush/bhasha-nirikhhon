/*
 Diseñe e implemente una Clase Manual Lubricante, que entrega los siguientes
atributos :tipo_lubricante de tipo String y viscosidad_lubricante de tipo decimal; e implemente los métodos get
, set y to String ,un constructor sobrecargado y otro constructor que permita inicializar las variables de la clase ,
adicionalmente ,implemente 2 métodos especiales .
Ingresar_Valor(float  viscosidad_lubricante) se ingresa el valor al manual de lubricante ,si la cantidad introducida
es negativa nó se hará nada ;por lo que se aceptará el ingreso del valor en el manual.
Identidicar_Valor(float viscosidad_lubricante) identifique la viscosidad del lubricante del manual de
lubricante,considerando que ,si restando la cantidad actual a la que nos pasan es negativa ,la viscosidad
del lubricante pasa a ser un cero.

 */
package manual_lubricanteapp;

/**
 *
 * @author Lenin Paul
 */
class Manual_Lubricante{
    //Atributos
private String tipo_lubricante;
private double viscosidad_lubricante;
//constructor
public Manual_Lubricante(String tipo_lubricante){
    this(tipo_lubricante , 0);
}
public Manual_Lubricante(String tipo_lubricante , double viscosidad_lubricante){
    this.tipo_lubricante=tipo_lubricante;
if(viscosidad_lubricante<0){
    this.viscosidad_lubricante=0;
}else{
    this.viscosidad_lubricante=viscosidad_lubricante;
}
}
 //metodos
public String getTipo_lubricante(){
    return tipo_lubricante;
}
public void setTipo_lubricante(String tipo_lubricante){
    this.tipo_lubricante=tipo_lubricante;
}
public double getViscosidad_lubricante(){
    return viscosidad_lubricante;
}
public void setViscosidad_lubricante(double viscosidad_lubricante){
    this.viscosidad_lubricante=viscosidad_lubricante;
}

public void Ingresar_Valor(double viscosidad_lubricante){
    if(viscosidad_lubricante>0){
        this.viscosidad_lubricante+=viscosidad_lubricante;
    }
}

public void Buscar_Valor(double viscosidad_lubricante) {
    if(viscosidad_lubricante<0){
        this.viscosidad_lubricante=viscosidad_lubricante;
    }else{
       this.viscosidad_lubricante-=viscosidad_lubricante;
    }
}
@Override
public String toString(){
    return " El  "+ tipo_lubricante +" tiene "+ viscosidad_lubricante +
             "  en el manual de lubricantes";

}
}

public class Manual_LubricanteApp {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
      Manual_Lubricante manual1= new Manual_Lubricante("Sintetico");
      Manual_Lubricante manual2= new Manual_Lubricante("semisintetico",3.2);
      manual1.Ingresar_Valor(0.25);
      manual2.Ingresar_Valor(0.5);

      manual1.Buscar_Valor(0.75);
      manual2.Buscar_Valor(0.1);

        System.out.println(manual1);
        System.out.println(manual2);







    }

}