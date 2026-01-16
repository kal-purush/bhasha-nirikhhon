/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package Controlador.seguridad;

/**
 *
 * @author cdavi
 */
public class RelPerfApl {

        public RelPerfApl(int aplicacion_codigo,int perfil_codigo,String consultar, String actualizar,String eliminar,String imprimir,String insertar) {
        this.aplicacion_codigo = aplicacion_codigo;
        this.perfil_codigo = perfil_codigo;
        this.consultar = consultar;
        this.actualizar = actualizar;
        this.eliminar = eliminar;
        this.imprimir = imprimir;
        this.insertar = insertar;
    }

    public int getAplicacion_codigo() {
        return aplicacion_codigo;
    }
    
    public int getPerfil_codigo() {
        return perfil_codigo;
    }

    public String getConsultar() {
        return consultar;
    }

    public String getActualizar() {
        return actualizar;
    }
    
     public String getEliminar() {
        return eliminar;
    }
     
      public String getImprimir() {
        return imprimir;
    }
      
       public String getInsertar() {
        return insertar;
    }

    public void setAplicacion_codigo (int aplicacion_codigo) {
        this.aplicacion_codigo = aplicacion_codigo;
    }
    
    public void setPerfil_codigo (int perfil_codigo) {
        this.perfil_codigo = perfil_codigo;
    }

    public void setConsultar(String consultar) {
        this.consultar = consultar;
    }

    public void setActualizar(String actualizar) {
        this.actualizar = actualizar;
    }
    
     public void setEliminar(String eliminar) {
        this.eliminar = eliminar;
    }
     
      public void setImprimir(String imprimir) {
        this.imprimir = imprimir;
    }
      
       public void setInsertar(String insertar) {
        this.insertar = insertar;
    }

    @Override
    public String toString() {
        return "RelPerfApl{" + "aplicacion_codigo=" + aplicacion_codigo + ", perfil_codigo=" + perfil_codigo + ", consultar=" + consultar + ", actualizar=" + actualizar + ", eliminar=" + eliminar + ", imprimir=" + imprimir + ", insertar=" + insertar + '}';
    }
    
    int aplicacion_codigo;
    int perfil_codigo;
    String consultar;
    String actualizar;
    String eliminar;
    String imprimir;
    String insertar;
    
    public RelPerfApl() { //no contiene nada
    }

    
}