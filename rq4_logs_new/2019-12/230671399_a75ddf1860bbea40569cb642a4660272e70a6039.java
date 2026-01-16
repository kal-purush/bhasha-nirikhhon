/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.rookiebot.sentence;

import java.util.ArrayList;
import java.util.Random;

/**
 *
 * @author William
 */

/*
Preposition = Before Determiner or Pronoun; Of, After, before, on
Determiner = The, A
Noun = Thing, Building
Verb = Doing; Ran, Walk - Tenses to look out for
    Present = Am, Work, Run
    Past = Was, Talked, Ran
    Future = "Will " + Present
Pronoun = Before adverb or verb; They, He, I
Adverb = Before verb; nearly, quickly
Modal Verb = Possibility; must, shall, is + " be "
Adjective = Describes noun; tall, sweet, fast
Conjunction = Connector; But, If, And
Exclamation = Ah! No! Yes!
*/
public class RandomSentence {
    String point; /* 
    Point = 1-3 of combo
    */
    
    String counter; /*
    Counter = 1-3 of combo, add E at the end
    C,pN,V,P,N "But I walk the world"
    C,mV,pN,V,P,N "But shall I walk the world"
    c,pN,mV,V,P,N "But I shall walk the world"
    pN,V,P,N "I walk the world"
    C,pN,V "But I walk" 
    C,V,P,N "But walk the world"
    */

    ArrayList<String> p = new ArrayList<>();
    ArrayList<String> d = new ArrayList<>();;
    ArrayList<String> n = new ArrayList<>();;
    ArrayList<String> v = new ArrayList<>();;
    ArrayList<String> pN = new ArrayList<>();;
    ArrayList<String> aV = new ArrayList<>();;
    ArrayList<String> mV = new ArrayList<>();;
    ArrayList<String> aJ = new ArrayList<>();;
    ArrayList<String> c = new ArrayList<>();;
    ArrayList<String> e = new ArrayList<>();;
    
    
    
    public RandomSentence()
    {
        
    }
    
    public String getMessage()
    {
        String message = "";
        return message;
    }
    
    /*P,D,N "After the man"
    P,D,V "After a run"
    D,N,mV,aJ "The man is living"
    D,V,mV,aJ "The run is living"
    P,pN,V,A "After I run fast"
    P,pN,V "After I run"
    pN,V,A "I run fast"
    */
    public String gP(int i1, int i2, int i3)
    {
        ArrayList<String> comboPs = new ArrayList<>();
        comboPs.add(p.get(gN(p.size())) + " " + d.get(gN(d.size())) + " " + n.get(gN(n.size())) + " " + mV.get(gN(mV.size())) + " " + aJ.get(gN(aJ.size())) + ". ");
        comboPs.add(p.get(gN(p.size())) + " " + d.get(gN(d.size())) + " " + v.get(gN(v.size())) + " " + mV.get(gN(mV.size())) + " " + aJ.get(gN(aJ.size())) + ". ");
        comboPs.add(p.get(gN(p.size())) + " " + pN.get(gN(pN.size())) + " " + v.get(gN(v.size())) + " " + aV.get(gN(aV.size())) + ". ");
        return comboPs.get(i1) + comboPs.get(i2) + comboPs.get(i3);
    }
    
    public int gN(int max)
    {
        Random rand = new Random();
        return rand.nextInt(max);
    }
}