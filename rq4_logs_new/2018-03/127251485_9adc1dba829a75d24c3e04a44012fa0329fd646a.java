import java.lang.Math;

public class Calc {
	public int add (int a, int b){
		return a+b;
	}
	public double add (double a, double b){
		return a+b;
	}
	public int sub (int a, int b){
		return a-b;
	}
	public double sub (double a, double b){
		double c = a-b;
		return roundRes(c,3);
	}
	public int mul (int a, int b){
		return a*b;
	}
	public double mul (double a, double b){
		return a*b;
	}
	public int div (int a, int b){
		return a/b;
	}
	public double div (double a, double b){
		double c = a/b;
		return roundRes(c,3);
	}
	public double roundRes (double a, double pr){
		pr = Math.pow(10,pr);
		a = a*pr;
		int i = (int) Math.round(a);
		return (double) i/pr;
	}
}