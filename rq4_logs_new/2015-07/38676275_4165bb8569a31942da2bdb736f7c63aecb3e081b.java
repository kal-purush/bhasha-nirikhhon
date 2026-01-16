package calender;

public class BirthDay {

		 private int year;
		 private int month;
		 private int day;
		 
		 public BirthDay(int year, int month, int day){
			 this.year = year;
			 this.month = month;
			 this.day = day;
		 }
		 
	public boolean isLeapYear(){
		if(((year%4==0) && (year%100!=0)) ||(year%400==0)){
			return true;
		}
		else{ 
			return false;
		
		}
	}
			
		public boolean is30days(){		
			if(month==4 || month==6 || month==9 || month==11){
				return true;
			}else{
				return false;
			}	
		}
		
		
			public void checkValidity() {				
	
				if(year<1 || month<1 || month>12 || day<1 || day>31){
					System.out.println("무효");
					}
				if(month==2 && day>29){
					System.out.println("무효");
					}
				if(month==2 && day==29 && !isLeapYear()){
					System.out.println("무효");
					}	
				if(day==31 && is30days()){
					System.out.println("무효");
					}
	
				}
}