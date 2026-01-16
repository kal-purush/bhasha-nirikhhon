/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package bankersalgorithm;

import java.util.Scanner;

/**
 *
 * @author rub account
 */
public class BankersAlgorithm {

    /**
     * @param args the command line arguments
     */
    int [][] assigned,needed;
    boolean[] isTerminated;
    int[] E,P,A;
    public BankersAlgorithm(int nProcesses,int nResources,int[] e,int[] p, int[] a){
        assigned=new int[nProcesses][nResources];
        needed=new int[nProcesses][nResources];
        E=e;
        P=p;
        A=a;
        isTerminated=new boolean[nProcesses];
        for (int i = 0; i < nProcesses; i++) {
            isTerminated[i]=false;
        }
    }
    public void processRequest(int pid, int[] requestedResources){
        for (int i = 0; i < needed[0].length; i++) {
            needed[pid][i]+=requestedResources[i];
        }
    }
    public boolean isSufficient(int process){
        for (int i = 0; i < needed[0].length; i++) {
            if(needed[process][i]>A[i]){
                return false;
            }
        }
        return true;
    }
    void freeResources(int process){
        for (int i = 0; i < needed[0].length; i++) {
            A[i]+=needed[process][i];
            needed[process][i]=0;
        }
        isTerminated[process]=true;
        for (int i = 0; i < needed.length; i++) {
            for (int j = 0; j < needed[0].length; j++) {
                System.out.print(needed[i][j]+",");
            }
            System.out.println("");
        }
        System.out.print("Available Resources:");
        for(int i:A)
            System.out.print(i+",");
        System.out.println("");
        System.out.println("process "+process+" terminated");
        System.out.println("--------------");
    }
    public boolean isAllComplete(){
        for (int i = 0; i < needed.length; i++) {
            if(!isTerminated[i])
                return false;
        }
        return true;
    }
    public boolean isSafeState(){
        if(isAllComplete()){
            return true;
        }
        for (int i = 0; i < needed.length; i++) {
            if(!isTerminated[i]&&isSufficient(i)){
                freeResources(i);
                return isSafeState();
            }
        }
        return false;
    }
    
    public static void main(String[] args) {
        
        Scanner sc=new Scanner(System.in);
        int nProcesses,nResources,pID;
        int[] availableResources,request;
        System.out.println("Enter number of processes");
        String lineEntered=sc.nextLine();
        nProcesses=Integer.valueOf(lineEntered);
        System.out.println("Enter available resources");
        lineEntered=sc.nextLine();
        String[] split = lineEntered.split(",");
        nResources=split.length;
        availableResources=new int[nResources];
        for (int i = 0; i < nResources; i++) {
            availableResources[i]=Integer.valueOf(split[i]);
        }
        BankersAlgorithm b=new BankersAlgorithm(nProcesses,nResources,null,null,availableResources);
        System.out.println("Enter process ID");
        pID=Integer.valueOf(sc.nextLine());
        do{
            System.out.println("Enter process request");
            lineEntered=sc.nextLine();
            split=lineEntered.split(",",nResources);
            request=new int[nResources];
            for (int i = 0; i < nResources; i++) {
                request[i]=Integer.valueOf(split[i]);
            }
            b.processRequest(pID, request);
            System.out.println("Enter process ID");
            pID=Integer.valueOf(sc.nextLine());
        }while(pID>=0&&pID<nProcesses);
        if(b.isSafeState())
            System.out.println("all processes complete");
        else{
            System.out.println("DEADLOCK");
            for(int[] arr:b.needed){
                for(int i:arr){
                    System.out.print(i+",");
                }
                System.out.println("");
            }
        }
    }
    
}