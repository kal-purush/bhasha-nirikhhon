import java.util.Random;
import java.util.Scanner;

public class blackjack
{ //ace has one or eleven, your choice. all face cards also have 10 points
	public static void main(String[] args)
	{
		Scanner key = new Scanner(System.in);
		Random r = new Random();
		System.out.println("Welcome to Blackjack!");
		int first_card = r.nextInt(13) + 1;
		int second_card = r.nextInt(13) + 1;
		int third_card;
		String t_cardvalue="";
		String f_cardvalue="";
		String s_cardvalue="";
		int ace;
		if (first_card>=10) {first_card=10; f_cardvalue = "10";}
		if (second_card>=10) {second_card=10; s_cardvalue= "10";}
		if (first_card==1) {f_cardvalue= "Ace";}	
		if (second_card==1) {s_cardvalue= "Ace";}
		if((1<first_card) && first_card<10) {f_cardvalue = Integer.toString(first_card);}
		if((1<second_card)&& second_card<10) {s_cardvalue = Integer.toString(second_card);}	
		
		System.out.println("You get a "+ f_cardvalue + " and a " + s_cardvalue );
		if (f_cardvalue.equals("Ace")) { System.out.println("Do you want Ace to be a 1 or an 11?"); ace = key.nextInt(); first_card=ace;}
		if (s_cardvalue.equals("Ace")) { System.out.println("Do you want Ace to be a 1 or an 11?"); ace = key.nextInt(); second_card=ace;}
		int sum = first_card + second_card;
		System.out.println("Your total is " + Integer.toString(sum) + ".");

//now the dealer's turn..

		int d_first_card = r.nextInt(13) + 1;
		int d_sec_card = r.nextInt(13) + 1;
		int d_third_card; 
		String dt_cardvalue="";
		String df_cardvalue="";
		String ds_cardvalue="";
		if (d_first_card>=10) {d_first_card=10; df_cardvalue = "10";}
		if (d_sec_card>=10) {d_sec_card=10; ds_cardvalue= "10";}
		if (d_first_card==1) {df_cardvalue= "Ace";}	
		if (d_sec_card==1) {ds_cardvalue= "Ace";}
		if(10>d_first_card && d_first_card>1) {df_cardvalue = Integer.toString(d_first_card);}
		if(10>d_sec_card && d_sec_card>1) {ds_cardvalue = Integer.toString(d_sec_card);}
		System.out.println("\nThe dealer has a " + df_cardvalue + " showing. The second card is hidden.\nHis total is also hidden");
		if (df_cardvalue.equals("Ace") && ds_cardvalue.equals("Ace")) { d_first_card=1; d_sec_card=11;}
		if (!ds_cardvalue.equals("Ace") && (df_cardvalue.equals("Ace"))) { d_first_card=11;}
		if (ds_cardvalue.equals("Ace") && !(df_cardvalue.equals("Ace"))) { d_sec_card=11;}
		int d_sum = d_first_card + d_sec_card;
//play
		System.out.println("\nWould you like to hit or stay?");
		String ans= key.next();
		while (ans.equals("hit"))
		{
			third_card = r.nextInt(13) + 1;
			if (third_card>=10) {third_card=10; t_cardvalue = "10";}
			if (third_card==1) {t_cardvalue= "Ace";}
			if(10>third_card && third_card>1) {t_cardvalue = Integer.toString(third_card);}

			System.out.println("You drew a " + t_cardvalue );
			if (t_cardvalue.equals("Ace")) { System.out.println("Do you want Ace to be a 1 or an 11?"); ace = key.nextInt(); third_card=ace;}
			sum = sum + third_card;
			System.out.println("Your total is " + Integer.toString(sum));	
			if(sum>21) {System.out.println("You are busted! Dealer wins!"); return;}
			System.out.println("\nWould you like to hit or stay?");			
			ans = key.next();
			
		}
		System.out.println("\nOkay, dealer's turn. \nHis hidden card was " + ds_cardvalue + ". \nHis total is " + Integer.toString(d_sum));
		while (d_sum<= 16)
		{
			d_third_card = r.nextInt(13) + 1;
			if (d_third_card>=10) {d_third_card=10; dt_cardvalue = "10";}
			if (d_third_card==1) {dt_cardvalue= "Ace";}
			if(10>d_third_card && d_third_card>1) {dt_cardvalue = Integer.toString(d_third_card);}
			if (dt_cardvalue.equals("Ace")) { if (d_sum<=10){d_third_card=11;} else{d_third_card=1;}}			
			d_sum= d_sum + d_third_card;
			System.out.println("Dealer chooses to hit.\nHe draws a " + dt_cardvalue + ". \nHis total is " + Integer.toString(d_sum));
			if(d_sum>21) {System.out.println("Dealer busted! You win!"); return;}
		}
		System.out.println("Dealer stays.\nDealer's total is " + Integer.toString(d_sum) + ".\nYour total is " + Integer.toString(sum));
		if ((21-d_sum)<=(21-sum))
		{
			System.out.println("Dealer wins!");
		}
		else {System.out.println("You win!");}
	}
}