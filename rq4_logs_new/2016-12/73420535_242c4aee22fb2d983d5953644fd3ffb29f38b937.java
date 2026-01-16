/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jlab.dc_calibration;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JOptionPane;
import org.jlab.service.dc.DCHBEngine;
import org.jlab.service.dc.DCTBEngine;

/**
 *
 * @author kpadhikari
 */
public class RunReconstructionCoat3 implements ActionListener {

    //static java.io.File fileChosen = null; //kp: 11/30/16 
    private static String fileChosen = null; //kp: 11/30/16 
    private static String filePathOnly = null; //kp: 12/01/16
    private static String fileChosenFullPath = null; //kp: 11/30/16 
    
    private static String OS = System.getProperty("os.name").toLowerCase();

    public RunReconstructionCoat3() {
        System.out.println("Now running the reconstruction");;
    }
    
    public void actionPerformed(ActionEvent ev) {
        JFrame frame = new JFrame("JOptionPane showMessageDialog example1");

        // show a joptionpane dialog using showMessageDialog        
        JOptionPane.showMessageDialog(frame, "Click OK to choose the input file ...");

        JFileChooser fileChooser = new JFileChooser();
        int returnVal = fileChooser.showOpenDialog(null);
        //if (selectedFile != null) {
        if (returnVal == JFileChooser.APPROVE_OPTION) {
            java.io.File file = fileChooser.getSelectedFile();
            java.io.File fileD = fileChooser.getCurrentDirectory();
            fileChosen = file.getName();
            filePathOnly = fileD.getName();
            fileChosenFullPath = fileD.getAbsolutePath();
            System.out.println("File selected: " + fileChosen);
        } else {
            System.out.println("File selection cancelled.");
        }

        processData();
    }

    public void processData() {

        String inputFile = null;
        String fDir = fileChosenFullPath;//"C:\\Users\\KPAdhikari\\Desktop\\BigFls\\CLAS12";
        String fName = fileChosen;//"theDecodedFileR128T0corSec1_allEv.0";
        String oDir = fDir; //"C:\\Users\\KPAdhikari\\Desktop\\BigFls\\CLAS12";
        
        if (isWindows()) {
            //System.out.println("This system is Windows ...");
            inputFile = String.format("%s\\%s", fDir, fName);
        } else if (isMac()) {
            //System.out.println("This system is MacOS ...");
            inputFile = String.format("%s/%s", fDir, fName);
        } else if (isUnix()) {
            //System.out.println("This system is Unix/Linux ...");
            inputFile = String.format("%s/%s", fDir, fName);
        } else {
            System.out.println("This system is neither Windows, nor MacOS, nor Unix/Linux ...");
            System.out.println("Modify code accordingly. Program exiting for now.");
            System.exit(0);
        }
        
               
        String COATJAVA = System.getenv("CLAS12DIR");
        System.out.println("JAVA_HOME = " + System.getenv("JAVA_HOME"));
        System.out.println("CLAS12DIR = " + System.getenv("CLAS12DIR"));
       

        System.err.println(" \n[PROCESSING FILE] : " + inputFile);
        DCHBEngine en = new DCHBEngine();
        en.init();
        DCTBEngine en2 = new DCTBEngine();
        //DCTBRasterEngine en2 = new DCTBRasterEngine();
        en2.init();
        
        
        org.jlab.io.evio.EvioSource reader = new org.jlab.io.evio.EvioSource();
        int counter = 0;
        reader.open(inputFile);
        
        long t1 = System.currentTimeMillis();
        while (reader.hasEvent()) {

            counter++;
            org.jlab.io.evio.EvioDataEvent event = (org.jlab.io.evio.EvioDataEvent) reader.getNextEvent();
            en.processDataEvent(event);

            // Processing TB  
            en2.processDataEvent(event);

            //if(counter>5) break;
            //if(counter%100==0)
            //        System.out.println("run "+counter+" events");
        }
        double t = System.currentTimeMillis() - t1;
        System.out.println("TOTAL  PROCESSING TIME = " + t);

        System.out.println("Finished running reconstruction for this iteration ...");

    }
    
    private static boolean isWindows() {
        return (OS.indexOf("win") >= 0);
    }

    private static boolean isMac() {
        return (OS.indexOf("mac") >= 0);
    }

    private static boolean isUnix() {
        return (OS.indexOf("nux") >= 0);
    }
}