/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package adtarrayempresa;



/**
 *
 * @author HP
 */
public class ADTarrayEmpresa {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here
        //LeerFichero fichero = new LeerFichero();
        //fichero.leerArchivo();
        Nomina prueba =new Nomina("hola");
        //System.out.println(prueba
        prueba.listarSueldos();
        System.out.println("Mayor antiguedad = "+prueba.mayorAntiguedad());
        
        
        
    }

}
