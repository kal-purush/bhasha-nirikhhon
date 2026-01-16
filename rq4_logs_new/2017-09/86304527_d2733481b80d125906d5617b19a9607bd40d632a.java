package uk.abdn.cs.semanticweb.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLAxiom;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;

import uk.ac.manchester.cs.owl.owlapi.OWLClassImpl;

public class GeneralInfo {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {		
		
		PrintStream out = new PrintStream(new FileOutputStream("C:/Users/Isa/Desktop/Details_of_8805_ontologies.txt"));
		System.setOut(out);
		
		GeneralInfo dCls = new GeneralInfo();
	
		File folder = new File("C:/Users/Isa/Desktop/ontologies1251");
		File[] listOfFiles = folder.listFiles();
		
		for (File file : listOfFiles) {
			if (file.isFile()) {
				countABoxTBox(file.getCanonicalPath());
			}
		}
		
	}
	
	public String infoFolder(String pFolderPath, String pFormat) {

		File folder = new File(pFolderPath);
		File[] listOfFiles = folder.listFiles();
		
		for (File file : listOfFiles) {
			if (file.isFile()) {
				
				System.out.println("--- General Info of an Ontology ---");
				long processTime = 0;
				long start = System.nanoTime(); 
				
				if (pFormat.equalsIgnoreCase("General")){
					consoleAxiomTypesGeneral(pFolderPath + "/" + file.getName());
				}else if (pFormat.equalsIgnoreCase("Logical")){
					consoleAxiomTypesLogical(pFolderPath + "/" + file.getName());
				}
 				
				processTime = System.nanoTime() - start;
				System.out.println("--- End of Task. (" + (processTime/1000000) + " msec.) ---");
			}
		}

		return "";
	}

	public static void countABoxTBox(String pFileName){		
		try {

			File ontFile = new File(pFileName);
			OWLOntologyManager om = OWLManager.createOWLOntologyManager();
			OWLOntology ont = om.loadOntologyFromOntologyDocument(ontFile);
			double TBoxAxioms = ont.getTBoxAxioms(true).size();
			System.out.println("Ontology:" + ontFile.toString() + ":" + ontFile.toString().substring(0, ontFile.toString().lastIndexOf("_")) + ".owl" + ":" 
					+ ontFile.toString().substring(ontFile.toString().lastIndexOf("_") + 1, ontFile.toString().lastIndexOf(".")) + ": AxiomCount:"
					+ ont.getAxiomCount() + ": Logical:"
					+ ont.getLogicalAxiomCount() + ": TBox:" + TBoxAxioms
					+ ": ABox:" + ont.getABoxAxioms(true).size()
					+ ": ABox/TBox:"
					+ ((double) ont.getABoxAxioms(true).size() / TBoxAxioms));

		} catch (Exception e) {
			e.printStackTrace();
		} 
	}

}