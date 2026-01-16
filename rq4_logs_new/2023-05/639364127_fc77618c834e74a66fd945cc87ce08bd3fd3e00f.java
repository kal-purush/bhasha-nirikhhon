/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package mib.projektet;

import javax.swing.JOptionPane;
import javax.swing.JPasswordField;
import javax.swing.JTextField;
import oru.inf.InfDB;
import oru.inf.InfException;

/**
 *
 * @author oskarjolesjo
 */
public class Validering {
    
  public static boolean kollaTomRuta(JTextField angeEpost) {
      boolean tomRuta = true;
      
      if ((angeEpost.getText().isEmpty())){
          JOptionPane.showMessageDialog(null, "Epost ej ifyllt!");
          
          tomRuta = false;        
      }
     
      return tomRuta;
  }
  public static boolean kollaTomRuta2(JPasswordField angeLosenord) {
      boolean tomRuta = true;
      
      if ((angeLosenord.getText().isEmpty())){
          JOptionPane.showMessageDialog(null, "Lösenord ej ifyllt!");
          
          tomRuta = false;        
      }
     
      return tomRuta;
  }
   public static boolean kollaTomRuta3(JTextField gamlaLosenord) {
      boolean tomRuta = true;
      
      if ((gamlaLosenord.getText().isEmpty())){
          JOptionPane.showMessageDialog(null, "Gamla lösenordet ej ifyllt!");
          
          tomRuta = false;        
      }
     
      return tomRuta;
   }
      
      public static boolean kollaTomRuta4(JTextField nyaLosenord) {
      boolean tomRuta = true;
      
      if ((nyaLosenord.getText().isEmpty())){
          JOptionPane.showMessageDialog(null, "Nya Lösenordet ej ifyllt!");
          
          tomRuta = false;        
      }
     
      return tomRuta;
      }
      
 public static boolean valideraTelefonnummer(String telefonnummer) {
        // Validera att telefonnummer endast innehåller siffror
        return telefonnummer.matches("\\d+");
    }
 
 public static boolean valideraOmradeID(String omradeID) {
        // Validera att omradeID är antingen 1, 2 eller 4
        return omradeID.matches("[124]");
    }
 
 public static boolean kollaAdminStatus(String adminStatus) {
        if (adminStatus.equals("J") || adminStatus.equals("N")) {
            return true;
        } else {
            return false;
        }
    
}
 public static boolean agentFinnsIDatabas(InfDB idb, String agent) {
    try {
        String sqlFraga = "SELECT * FROM agent WHERE Agent_ID = " + agent;
        String resultat = idb.fetchSingle(sqlFraga);
        return (resultat != null);
    } catch (InfException e) {
        e.printStackTrace();
        return false;
    }
}
 
  
 
}
