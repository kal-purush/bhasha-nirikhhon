package rekop;

public class poker {
	 
public static int HighCard(int[] nu) {
		int j=0;
		for(int i=12;i>-1;i--){
		if(nu[i]==1){
			 j=i;
			break;
		}
		}
		return j;	
	}
public static boolean Onepair(int[] nu) {
   boolean a= false;
	for(int i=0;i<13;i++){
	   if(nu[i]==2){
		   a= true;
		   break;
	   }
   }
	return a;
}
public static boolean Twopair(int[] nu) {
	int count=0;
	for(int i=0;i<13;i++){
		   if(nu[i]==2){
			   count++;
		   }
	   }
	if(count==2){
	return true;
	}
	else{
		return false;
	}
}
public static boolean ThreeOfaKind(int[] nu) {
	boolean a=false;
	for(int i=0;i<13;i++){
		   if(nu[i]==3){
			   a= true;
			   break;
		   }
	 }
	return a;
}
public static boolean Straight(int[] nu){
	boolean a=false;
	for(int i=0;i<9;i++){
		if(nu[i]>1){
			a= false;
			break;
		}
		else if (nu[i] ==1){
			if(i==0){
				if(nu[1]!=1 || nu[12]!=1){
					a= false;
					break;
				}
				else{
				    if((nu[1]==1 && nu[2]==1 && nu[3]==1 && nu[4]==1) || (nu[12]==1 && nu[11]==1 && nu[10]==1 &&nu[9]==1)){
				     a= true;
				     break;
				    }
				}
			}
			else {
				if(nu[i+1]==1 && nu[i+2]==1 && nu[i+3]==1 && nu[i+4]==1){
					a= true;
					break;
				}
			}
		  }
	 }
	return a;
}
public static boolean Flush(int[] su) {
	boolean a= false;
	for(int i=0;i<4;i++){
		if(su[i]==5){
			a=true;
			break;
		}
	}
	return a;
}
public static boolean FullHouse(int[] nu){
	int count_3=0;
	int count_2=0;
	boolean a=false;
	for(int i=0;i<13;i++){
		   if(nu[i]==3){
			   count_3++;
		   }
		   else if(nu[i]==2){
			   count_2++;  
		   }
    if(count_2==1 && count_3==1){
		  a=true;
	}  
	 }
	return a; 
}
public static boolean FourOfaKind(int[] nu) {
	boolean a=false;
	for(int i=0;i<13;i++){
		   if(nu[i]==4){
			   a=true;
		   }	
	 }
	return a;
}

public static boolean StraightFlush(int[] su,int[] nu){
	
	if(poker.Flush(su) && poker.Straight(nu)){
		return true;
	}
	else{
		return false;
	}
}
public static boolean RoyalFlush(int[] su,int[] nu){
	if(poker.Flush(su) && poker.Straight(nu) && nu[0]==1 && nu[12]==1){
		return true;
	}
	else{
		return false;
	}	
}
}