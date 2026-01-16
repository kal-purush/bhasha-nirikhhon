package org.darioaxel.transformator;

public class PowerScriptToKDM {

	public static void main(String[] args) {
		
		System.out.println("zero" + args[0] + "\n" +"one"+ args[1]);
		Transformator trans = new Transformator(args[0], args[1]);
		trans.Transform();
		System.out.println("ended");
	}
}