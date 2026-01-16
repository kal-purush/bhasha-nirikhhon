package org.inter;

public class IciciBank implements RBIBank {

	@Override
	public void deposit() {
		System.out.println("6%");
		
	}

	@Override
	public void fixed() {
		System.out.println("8%");
		
	}

	@Override
	public void saving() {
		System.out.println("10%");
		
	}
	public static void main(String[] args) {
		IciciBank b=new IciciBank();
		b.deposit();
		b.fixed();
		b.saving();
	}

}