package basic.bean;

/**
 * Created by Alex on 27.10.2016.
 */

import basic.interf.Symbol;


import javax.ejb.Remote;
import javax.ejb.Stateless;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.TypedQuery;
import java.util.HashMap;

@Remote(Symbol.class)
@Stateless
public class SymbolBean implements Symbol {

    @Override
    public HashMap<Character,Integer> count(String str){
        HashMap<Character,Integer> hm=new HashMap<Character, Integer>();
        for(int i=0; i<str.length();i++){
            if(hm.containsKey(str.charAt(i))){
                hm.put(str.charAt(i),hm.get(str.charAt(i))+1);
            }
            else{
                hm.put(str.charAt(i),1);
            }
        }
        return hm;
    }
}