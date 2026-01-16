/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jlab.dc_calibration;

import org.jlab.service.dc.DCHBEngine;
import org.jlab.service.dc.DCTBEngine;

/**
 *
 * @author KPAdhikari
 */
public class RecTestCoatjava30 {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here

        int inFileNum = 1;
        String inputFile = null;
        //String fDir = "C:\\Users\\KPAdhikari\\Desktop\\BigFls\\CLAS12"; //In Windows
        String fDir = "/home/kpadhikari/Desktop/BigFls/CLAS12"; //In Ubuntu 

        //String fName = "theDecodedFileR128T0corSec1_allEv.0"; //this file not used anymore
        //String fName = "theDecodedFileR128T0corSec1_allEv.0_header"; //not used anymore
        String fName = "theDecodedFileR128T0corSec1_30k.0_header";
        
        //inputFile = String.format("%s\\%s.evio", fDir, fName);
        inputFile = String.format("%s/%s.evio", fDir, fName);
        
        
        //String outputFile = "/Users/ziegler/Workdir/Distribution/coatjava-3.0.1/DCRBREC.evio";
        //String outputFile = String.format("%s\\recOutDCRBREC.evio", fDir);
        String outputFile = String.format("%s/recOutDCRBRECub.evio", fDir);
        String line;
        
        //String COATJAVA = "C:\\Users\\KPAdhikari\\Box Sync\\VMWareSharedDocs\\Java\\COATJAVA\\coatjava-2.4\\coatjava";
        String COATJAVA = System.getenv("CLAS12DIR");
        System.out.println("JAVA_HOME = " + System.getenv("JAVA_HOME"));
        System.out.println("CLAS12DIR = " + System.getenv("CLAS12DIR"));

        /*
        String RecOpt1 = "-s DCHB:DCTB -config TIME::T0=0 -config DATA::mc=false";
        String RecOpt2 = "-config DAQ::data=true -config CCDB::VAR='dc_test' -config LAYEREFFS::on=true";
        String oDir = "C:\\Users\\KPAdhikari\\Desktop\\BigFls\\CLAS12";
        String recIP = null, recOP = null, command = null;
        recIP = String.format("%s\\theDecodedFileR128T0corSec1_allEv.0.evio", oDir);
        recOP = String.format("%s\\recOpR128T0corT2DfromCCDBvarFit02.evio", oDir);
        command = String.format("%s\\bin\\clas12-reconstruction %s %s -i %s -o %s -n 30000",
                COATJAVA, RecOpt1, RecOpt2, recIP, recOP);
        System.out.println("The command to be run now is: \n" + command);
        command = String.format("%s\\bin\\clas12-reconstruction %s %s -i %s -o %s -n 30000",
                System.getenv("CLAS12DIR"), RecOpt1, RecOpt2, recIP, recOP);
        
        System.out.println("The command to be run now is: \n" + command);

        String[] recArgs = {"-s", "DCHB:DCTB", "-config", "TIME::T0=0", "-config", "DATA::mc=false",
            "-config", "DAQ::data=true", "-config", "CCDB::VAR='dc_test'", "-config",
            "LAYEREFFS::on=true", "-i", recIP, "-o", recOP, "-n", "30000"};
        org.jlab.clasrec.rec.CLASReconstruction rec = new org.jlab.clasrec.rec.CLASReconstruction();//coatjava2.4
        //org.jlab.rec.dc.CLASReconstruction rec = new org.jlab.rec.dc.CLASReconstruction();
        rec.main(recArgs);
         */
        //String inputFile = "/Users/ziegler/Workdir/Distribution/coatjava-3.0.1/output_with_header.0.evio";
        //String inputFile = args[0];
        //String outputFile = args[1];
        
        
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

        //Writer
        //String outputFile="/Users/ziegler/Workdir/Distribution/coatjava-3.0.1/DCRBREC.evio";
        org.jlab.io.evio.EvioDataSync writer = new org.jlab.io.evio.EvioDataSync();
        writer.open(outputFile);

        while (reader.hasEvent()) {

            counter++;
            org.jlab.io.evio.EvioDataEvent event = (org.jlab.io.evio.EvioDataEvent) reader.getNextEvent();
            en.processDataEvent(event);

            // Processing TB  
            en2.processDataEvent(event);

            //if(counter>5) break;
            //if(counter%100==0)
            //        System.out.println("run "+counter+" events");
            writer.writeEvent(event);
        }
        writer.close();
        double t = System.currentTimeMillis() - t1;
        System.out.println("TOTAL  PROCESSING TIME = " + t);
    }
}