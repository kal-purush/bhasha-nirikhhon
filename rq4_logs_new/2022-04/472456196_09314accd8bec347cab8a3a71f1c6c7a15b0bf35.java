package catalog;
class BearNeccessitiesCustomer{//Customer class
	private String username;
	private String level;
	public BearNeccessitiesCustomer(){
		
	}
	public BearNeccessitiesCustomer(String username,String level){
		this.username=username;
		if(level.equals("Supreme Diamond VIP")||level.equals("Enjoy Platinum VIP")||level.equals("Ordinary users"))
		this.level=level;
		else
		System.out.println("Error in grade information");
	}
	public void setUsername(String username){
		this.username=username;
	}
	public String getUsername(){
		return username;
	}
	public void setLevel(String level){
		if(level.equals("Supreme Diamond VIP")||level.equals("Enjoy Platinum VIP")||level.equals("Ordinary users"))
		this.level=level;
		else
		System.out.println("Error in grade information");
	}
	public String getLevel(){
		return level;
	}
	public String toString(){
		return "User name:"+getUsername()+",The user level is:"+getLevel();
	}
}