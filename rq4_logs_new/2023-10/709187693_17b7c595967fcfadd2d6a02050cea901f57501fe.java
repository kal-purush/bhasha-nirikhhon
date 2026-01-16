import java.util.Scanner;
import java.util.Random;
public class NumbergameTask1 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("Yo! welcome");
		int highscore=0;
		boolean nexttime=true;
		
		Random randomnumber=new Random();
		//System.out.println(x);
		
		while(nexttime) {
			
			int guessthisnumber=randomnumber.nextInt(100)+1;
			Scanner scan=new Scanner(System.in);
			int score=100;
			int count=10;
			
			while(count>0) {
				System.out.println("ENTER YOUR GUESS!!");
				int guessednumber=scan.nextInt();
				
				if(guessthisnumber==guessednumber) {
					System.out.println("That's right you got it");
					System.out.println("And your score is :"+score);
					break;
				}
				else {
					   if(guessthisnumber>guessednumber)
	                    {
	                        
	                        System.out.println("Guessed number is too Low");
	                        score-=10;
	                        
	                    }
	                    else
	                    {
	                        score-=10;
	                        System.out.println("Guessed Number is too High");
	                    
	                    }
				}
			}
			count--;
			
			
		
		if(highscore<score)
        {
            
             highscore=score;
        
        }
  
        //System.out.println("Current Score "+score);
		
        System.out.println("Highest Score "+highscore);
        
        System.out.print("Wanna play play again [yes: 1 || No: 0]: ");
       //String scan2=scan.next();
        int nextround=scan.nextInt();

        if(nextround==1){
            
            nexttime=true;
        
        }
        else{
         
            break;   
        
        }
    }
    
	
    System.out.println("Arigatou gozai mashi ta");
	}


	}