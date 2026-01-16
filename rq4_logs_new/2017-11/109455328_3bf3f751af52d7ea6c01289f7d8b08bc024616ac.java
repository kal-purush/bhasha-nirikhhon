package gmit.ie;

import java.io.*;
import java.util.*;

public class Runner {
	static Scanner userInput = new Scanner(System.in);
		
	public static void main(String[] args) {
		ArrayList<Process> processes = new ArrayList<>();  
        
		System.out.println("Please enter the amount of processes ");
        int processNo = userInput.nextInt();
        
        int i;
        for (i = 0; i < processNo; i++)
        	processes.add(new Process("", 0, 0, 0));
        
        // Process[] processes = new Process[processNo]; //creates an array for FCFS AND SJF giving it the no. of processes
        //processes = inputProcesses(processNo, processes);
        inputProcesses(processNo, processes);
        //processes.get(i).display();
        
        int choice;
    	//Input the choice from the user as chosen below
        System.out.println("\tPlease enter one of the 4 options below.");
        System.out.println("Enter your input number");
        System.out.println("Press 1 for round robin");
        System.out.println("Press 2 for FCFS");
        System.out.println("Press 3 for SJF");
        System.out.println("Press 4 to exit");
        choice = userInput.nextInt();
        
        switch (choice) {
	        case 1:
	        	roundR(processes);
	        	choice = 4;
	        	break;
	        case 2:
	        	fcfs(processes);
	        	choice = 4;
	        	break;
	        case 3:     
	        	Collections.sort(processes, new Process.burstComparator());	//sort
	        	sjf(processes);
	        	choice = 4;
	        	break;
	        default:
	        	choice = 4;
	        	break;
        }  //switch      
        
	}//main
	
	//System.out.println(processes.get(i).toString());
	
	
	// Round Robin Methods
	
	//Round Robin Main
	private static void roundR(ArrayList<Process> processes) {
		int currentTime, waitTime;
		
		System.out.println("Enter Quantum");
		final int quantum = userInput.nextInt();
		
		currentTime = 0;
		while (!finished(processes)) {
			
			for(int i = 0; i < processes.size(); i++) {				
				
				if(!processes.get(i).isAlive()) {
					continue; // skip any non-alive processes.
				}
				 
				Process current = processes.get(i); // this process is still alive.
				current.setWaitTime(currentTime);
				
				
				int duration;
                // get the smaller of the quantum and remaining time
                if(current.getRemainingTime() < quantum){
                    duration = current.getRemainingTime();
                } else{
                    duration = quantum;
                }

                // now duration will be the smaller of the 2.
                current.wait(duration);
                currentTime += duration;  
                
                
			}				 
			
		}//while
		
		for (int i = 0; i < processes.size(); i++) {
			System.out.println(processes.get(i).toString());
		}
		double average = roundRobinAverage(processes, quantum);		
		outputAverage(average);		
		
		
		
	}//rr

	//Finished 
    public static boolean finished(ArrayList<Process> processes){
        for(int i = 0; i < processes.size(); i++){
            if(processes.get(i).isAlive()){
                return false;
            }//if
        }//for
        return true;
    }//finished
    
    //Round Robin Average
    public static double roundRobinAverage(ArrayList<Process> processes, int quantum){
        double total = 0;
        
        for(int i = 0; i < processes.size(); i++){
            Process current = processes.get(i);
            total += rrAverageForOneProcess(current, quantum);
        }

        return total / processes.size();
    }//roundRobinAverage

   
    //Average for one process
    public static double rrAverageForOneProcess(Process process, int quantum){
        int lastWaitTime = process.getWaitTime();
        int numQuantums = process.getNumCycles() - 1;
        return lastWaitTime - numQuantums * quantum;
    }//rrAverageForOneProcess
    
	
    
    // First Come First Served Methods
    
    //First Come First Served Main
	private static void fcfs(ArrayList<Process> processes) {
		int i;
		int burst, currentTime, waitTime;
		processes.get(0).setWaitTime(0); //make the first element in the array equal to 0

		waitTime = 0;
        //Loop through the array until i reaches the no. of elements in the list chosen by the user (processNo)
        for (i = 0; i < processes.size(); i++) {
        	
            waitTime = calculateWaitTime(waitTime, i, processes); //make wait time equal to the new wait time calculated in the function below
            System.out.println(processes.get(i).toString());
        }//for	
        
        double average = FcfsRrAverage(processes);
        outputAverage(average);        
				
	}//fcfs

	
	//Shortest Job First Main
	private static void sjf(ArrayList<Process> processes) {
		fcfs(processes);
		double average = FcfsRrAverage(processes);
		
		
	}//sjf
	
	//Wait Time Calculus for FCFS and SJF
    public static int calculateWaitTime(int wait, int i, ArrayList<Process> processes) {

        //As long as i is not 0 execute the if statement
        if (i != 0) {
            wait += processes.get(i-1).getBurst(); //the next wait becomes the previous burst
            processes.get(i).setWaitTime(wait); //waitTime is set to the wait value
            processes.get(i).setCurrentTime(wait); //currentTime is set to the wait value
        }

        return processes.get(i).getWaitTime(); //waitTime is returned to be used again
        
    }//calculateWaitTime

    //Average
	private static double FcfsRrAverage(ArrayList<Process> processes) {
        //Variables
        double total, average;

        //Initialize
        total = average = 0;

        //Loops through until i is smaller than the no. of processes (processCount)
        for (int i = 0; i < processes.size(); i++)
            total += processes.get(i).getWaitTime();	//it gets the wait time total
        return average = total / processes.size(); //it calculates the wait time average
	}

	
    // Inputs    
    
    //Input and Output methods
	private static ArrayList<Process> inputProcesses(int processNo, ArrayList<Process> processes) {
        String processName;
        int burst, currentTime, waitTime;
        int i = 0;
      
		for (Process p: processes) {
			System.out.println("Enter process name");
			p.setProcessName(userInput.next());
			
			System.out.println("Enter burst time");
			p.setBurst(userInput.nextInt());
			
			//(Classname variableName: arrayname) in a for loop loops through array elements
			//processes.get(i) //get index for array list
			//processes.add(new Process(processName, burst, 0, 0));			
		}
		
		return processes;
		
	}//inputProcesses
	
	private static PrintStream outputAverage(double average) {
		PrintStream str;
		return str = System.out.printf("\n\tWait Time Average is %.2f\n", average);
	}
	
}//Runner