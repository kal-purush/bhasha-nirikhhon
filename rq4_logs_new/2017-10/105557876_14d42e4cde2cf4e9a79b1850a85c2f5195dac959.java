package Calculate;

public class Calculator {
    private int so1;
    private int so2;
    
	public Calculator() {
		
	}
	public int getSo1() {
		return so1;
	}
	public void setSo1(int so1) {
		this.so1 = so1;
	}
	public int getSo2() {
		return so2;
	}
	public void setSo2(int so2) {
		this.so2 = so2;
	}
    
	public int getSum(){
		return so1+so2;
	}
}