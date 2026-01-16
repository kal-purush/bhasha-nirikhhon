/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ClassDNA;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.DefaultListModel;
import javax.swing.JList;
import javax.swing.ListModel;

/**
 *
 * @author Blythe
 */

public class Catalizador {
    
    private File carpeta;
    private File rutaV, rutaF;    //rutas de los archivos Verdaderos
    private String RV, RF;
    private String nombreArchivoPositivo,nombreArchivoNegativo;
    private String hebra, tipoFalso;
    private int cantidad;
    private String tipo;
    
    private EscribirArchivo arc;
    private final LeerArchivo arc1 = new LeerArchivo();
    
    private final boolean band1 = false, band2 = false;

    private final JList LAP = new JList();
    private final JList LAN = new JList();
    private DefaultListModel AP;
    private DefaultListModel AN;
    
    
    
    public boolean initParam(String hebra, String tipo, String tipoFalso, int cantidad){
        this.cantidad=cantidad;
        this.tipo=tipo;
        this.hebra = hebra;
        this.tipoFalso = tipoFalso;
        
        this.AP = new DefaultListModel();
        this.AN = new DefaultListModel();
        
        //preparación de las rutas
        if (this.tipo.compareTo("EI")==0) {
            this.rutaV = new File(this.tipo+"_GT/1Datos/Path.true");
            this.rutaF = new File(this.tipo+"_GT/1Datos/duplexW_EI/Path.false");
        } else if (this.tipo.compareTo("IE")==0) {
            this.rutaV = new File(this.tipo+"_AG/1Datos/Path.true");
            this.rutaF = new File(this.tipo+"_AG/1Datos/duplexW_IE/Path.false");
        } else if (this.tipo.compareTo("EZ")==0) {
            this.rutaV = new File(this.tipo+"/1Datos/Path.true");
            this.rutaF = new File(this.tipo+"/1Datos/duplexW_EZ/Path.false");
        } else if (this.tipo.compareTo("ZE")==0) {
            this.rutaV = new File(this.tipo+"/1Datos/Path.true");
            this.rutaF = new File(this.tipo+"/1Datos/duplexW_EZ/Path.false");
        }
        this.nombreArchivoPositivo = "duplex"+this.hebra+"_"+this.tipo+".txt";
        this.nombreArchivoNegativo = "duplex"+this.hebra+this.tipoFalso+"_"+this.tipo+".txt";
//        this.rutaV = new File("Datos/Path.true");
//        this.rutaF = new File("Datos/duplexW_EI/Path.false");
        this.RV = rutaV.getAbsolutePath().replaceAll("Path.true", "")+this.nombreArchivoPositivo;
        this.RF = rutaF.getAbsolutePath().replaceAll("Path.false", "")+this.nombreArchivoNegativo;
        System.out.println(RV);
        System.out.println(RF);
        
        // Archivos Positivos
        AP.addElement(RV);        
        this.LAP.setModel(AP);
        
        //Archivos Negativos
        AN.addElement(RF);
        this.LAN.setModel(AN);
        
//        this.positivo=positivo;
//        this.negativo=negativo;
        
//        this.archivosP.add("C:\\Users\\Blythe\\Desktop\\geneARFF-master\\geneARFF-master\\Datos\\duplexW_EI\\duplexWI_EI100");
//        
//        this.archivosN.add("C:\\Users\\Blythe\\Desktop\\geneARFF-master\\geneARFF-master\\Datos\\duplexW_EI");
//        
//        
        
        return true;
    }
    public boolean exec(){
        try {
            // TODO add your han,dling code here:
            
            if (this.tipo.compareTo("EI")==0) {
                carpeta = new File(this.tipo+"_GT/2Datos Formato Arff EI/DataEI.arff");
                System.out.println(carpeta.getPath());
                arc = new EscribirArchivo(carpeta.getPath(), true);
                arc.EscribirEnArchivo("@RELATION ExonIntron");
                System.out.println("Ingresó a ExonIntron y abrió el archivo: ");
            } else if (this.tipo.compareTo("IE")==0) {
                carpeta = new File(this.tipo+"_AG/2Datos Formato Arff IE/DataIE.arff");
                arc = new EscribirArchivo(carpeta.getPath(), true);
                arc.EscribirEnArchivo("@RELATION IntronExon");
            } else if (this.tipo.compareTo("EZ")==0) {
                carpeta = new File(this.tipo+"/2Datos Formato Arff EZ/DataEZ.arff");
                System.out.println(carpeta.getPath());
                arc = new EscribirArchivo(carpeta.getPath(), true);
                arc.EscribirEnArchivo("@RELATION ExonZona");
            } else if (this.tipo.compareTo("ZE")==0) {
                carpeta = new File(this.tipo+"/2Datos Formato Arff ZE/DataZE.arff");
                arc = new EscribirArchivo(carpeta.getPath(), true);
                arc.EscribirEnArchivo("@RELATION ZonaExon");
            }
            
            if (cantidad>0) 
                cantidad*=2;
            else{
                System.err.println("La sub cadena debe ser mayor a Cero (0)");
            }
                
                
            
            for(int i = 1; i <= cantidad; i++){
                arc.EscribirEnArchivo("@ATTRIBUTE B" + i + 
                        " {a,c,g,t}");
            }
            
            arc.EscribirEnArchivo("@ATTRIBUTE CLASS {0,1}");
            arc.EscribirEnArchivo("@DATA");
            
            //---------------------Datos Positivos------------------------

            //Iterator item = archivosP.iterator();
            boolean band = false;
            int indice_inf = 0, indice_sup = 0;
            
            if(band == false){//No seleccionaron ningun items
                
                
                ListModel lista = LAP.getModel();
                System.out.println(""+lista.getSize());
                for(int i = 0; i < lista.getSize(); i++){
                    //System.out.println(lista.getElementAt(i));
                    LeerArchivo arcp = new LeerArchivo(lista.getElementAt(i).toString());
                    int lineas = arcp.CantidadLineas();
                    String[] Data = new String[lineas];
                    Data = arcp.AbrirArchivo();
            
                    for (int j = 0; j < lineas; j++) {
                        String linea = Data[j];
                        String cadena1, cadena2;
                        if (linea.contains("p")) {
                            int k = linea.indexOf("p");
                            
                            
                            indice_inf = k -(cantidad);
                            indice_sup = k +((cantidad)+1)+4;
                            //System.out.println(linea.substring(indice_inf, indice_sup));
                            
                            if(indice_inf >= 0 && indice_sup >= 0){
                                //linea = linea.substring(indice_inf, indice_sup);
                                //linea = linea.replaceAll(",p,", ",");
                                cadena1 = linea.substring(indice_inf, k);
                                cadena2 = linea.substring(k+6, indice_sup);
                                linea = cadena1 + cadena2;
                                arc.EscribirEnArchivo(linea+",1");
                                //System.out.println(cadena1 + " " + cadena2);
                                //System.out.println(linea + ",1");
                            }else{
                                System.out.println(indice_inf + " " + indice_sup);
                            }//--if-else
                        }//--if
                    }//--for
                }//--for
            }//----if
            
            //---------------------Datos Negativos------------------------
            //System.out.println(LAN.getModel().getSize());
            if (LAN.getModel().getSize()>0) {                
                band = false;
                indice_inf = 0;
                indice_sup = 0;
                
                if(band == false){//No seleccionaron ningun items
                    
                    ListModel lista = LAN.getModel();
                    
                    for(int i = 0; i < lista.getSize(); i++){
                        //System.out.println(lista.getElementAt(i));
                        LeerArchivo arcp = new LeerArchivo(lista.getElementAt(i).toString());
                        int lineas = arcp.CantidadLineas();
                        String[] Data = new String[lineas];
                        Data = arcp.AbrirArchivo();
                
                        for (int j = 0; j < lineas; j++) {
                            String linea = Data[j];
                            String cadena1, cadena2;
                            
                            if (linea.contains("p")) {
                                int k = linea.indexOf("p");
                                indice_inf = k -(cantidad);
                                indice_sup = k +((cantidad)+1)+4;
                                //System.out.println(linea.substring(indice_inf, indice_sup));
                                if(indice_inf >= 0 && indice_sup >= 0){
                                    //linea = linea.substring(indice_inf, indice_sup);
                                    //linea = linea.replaceAll(",p,", ",");
                                    cadena1 = linea.substring(indice_inf, k);
                                    cadena2 = linea.substring(k+6, indice_sup);
                                    linea = cadena1 + cadena2;
                                    arc.EscribirEnArchivo(linea+",0");
                                    //System.out.println(linea + ",0");
                                }else{
                                    System.out.println(indice_inf + " " + indice_sup);
                                }//--if-else
                            }//--if
                        }//--for
                    }//--for
                }//----if
            }
            
            
        } catch (IOException ex) {
            Logger.getLogger(Catalizador.class.getName()).log(Level.SEVERE, null, ex);
        } catch (Exception ex) {
            Logger.getLogger(Catalizador.class.getName()).log(Level.SEVERE, null, ex);
        }
        return true;
    }
    
}