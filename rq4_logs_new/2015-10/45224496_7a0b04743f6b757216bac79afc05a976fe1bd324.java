package com.example.model;
import java.util.*;

/**
 *
 * @author Kosmidou Maria
 * @aem 1369
 */
public class GalaxyModel {
   public List getTypes(String type) {

     List types = new ArrayList();

     if (type.equals("milkyway")) {
       types.add("The Milky Way is a galaxy that contains our Solar System.Its name is derived from its appearance as a dim glowing band arching across the night sky whose individual stars cannot be distinguished by the naked eye.The term is a translation of the Latin via lactea, from the Greek (galaxías kýklos)");
     }
     else if (type.equals("andromeda")) {
       types.add("The Andromeda Galaxy, also known as Messier 31, M31, or NGC 224, is a spiral galaxy approximately 780 kiloparsecs (2.5 million light-years) from Earth.It is the nearest major galaxy to the Milky Way and was often referred to as the Great Andromeda Nebula in older texts. It received its name from the area of the sky in which it appears, the constellation of Andromeda, which was named after the mythological princess Andromeda.");
     }
     else if (type.equals("blackeye")) {
       types.add("The Black Eye Galaxy (also called Evil Eye Galaxy; designated Messier 64, M64, or NGC 4826) was discovered by Edward Pigott in March 1779, and independently by Johann Elert Bode in April of the same year, as well as by Charles Messier in 1780. It has a spectacular dark band of absorbing dust in front of the galaxy's bright nucleus, giving rise to its nicknames of the galaxy. ");
     }
     return(types);
   }
}