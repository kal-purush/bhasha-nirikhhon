/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package parcial;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Scanner;

/**
 *
 * @author Cristian Sarmiento
 */
public class Codificar {



    public Codificar() {
        
    }

    public String decodeText(String s1) {
        char[] sc = s1.toCharArray();
        for (int i = 0; i < s1.length(); i++) {
            if (sc[i]-65>=3){
                sc[i]=(char)((((int)sc[i]-68)%26)+65);
            }
            else{
                sc[i]=(char)((((int)sc[i]-65+23)%26)+65);}
        }
        return String.copyValueOf(sc);
    }
    


}