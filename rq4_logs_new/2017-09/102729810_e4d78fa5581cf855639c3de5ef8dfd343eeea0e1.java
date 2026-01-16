
public class PlaneSeat {
	private int seatId;
	private boolean assigned;
	private int customerId;
	
	public PlaneSeat(int seat_Id){
		seatId = seat_Id;
	}
	
	public int getSeatID(){
		return seatId;
	}
	
	public int getCustomerID()
	{
		return customerId;
	}
	
	public boolean isOccupied(){
		return assigned;
	}
	
	public void assign(int cust_id){
		assigned = true;
		customerId = cust_id;
	}
	
	public void unAssign(){
		assigned = false;
		customerId = 0;
	}
}