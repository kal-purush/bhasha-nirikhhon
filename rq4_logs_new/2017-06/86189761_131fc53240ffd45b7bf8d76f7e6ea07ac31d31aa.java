/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.ac.aut.ense701.gameModel;

/**
 *
 * @author OEM-PC
 */
public class Score implements Comparable{
        private Integer score;
        private String name;
        
        public Score(){
            
        }
        
        public Score(Integer score, String name){
            this.score = score;
            this.name = name;
        }
        
        public Integer getScore(){
            return this.score;
        }
        
        public String getName(){
            return this.name;
        }

    @Override
    public int compareTo(Object o) {
        Score s = (Score)o;
        if(this.score> s.score){
            return 1;
        }
        else if(this.score < s.score){
            return -1;
        }
        else{
            return 0;
        }
    }
    }