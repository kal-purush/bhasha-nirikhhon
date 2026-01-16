package com.example.cohalz.entropy;

/**
 * Created by cohalz on 15/06/25.
 */
public class Move{
    int prex,prey,nextx,nexty,val;
     public Move(int prex, int prey, int nextx, int nexty,int val){
        this.prex = prex;
         this.prey = prey;
         this.nextx = nextx;
         this.nexty = nexty;
         this.val = val;
    }
}