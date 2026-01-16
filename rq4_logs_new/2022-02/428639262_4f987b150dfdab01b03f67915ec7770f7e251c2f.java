package queue;

public class Programmers_developFunction {
	    public int[] solution(int[] progresses, int[] speeds) {
	        int[] count = new int[100];
	        int day = 0;
	        for(int i = 0; i < progresses.length; i++){
	            day = 0;
	            while(progresses[i] < 100){
	                progresses[i] += speeds[i];
	                day++;
	            }
	            count[day]++;
	        }
	        
	        int size = 0;
	        
	        for(int i: count){
	            if(i != 0){
	                size++;
	            }
	        }
	        
	        int[] answer = new int[size];
	        return answer;
	    }
	
}