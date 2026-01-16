package workspacejava;

public class Test1 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		/*
		int[] a = {1,4,5,6,8,9,10};
		System.out.println(a.length);
		for( int i=0;i<a.length;i++ ) {
			System.out.println("a["+i+"]= "+a[i]);
		}
		
		for(int i=0;i<args.length;i++) {
			System.out.println(args[i]);
		}
		*/
		if(args.length<3) {
			System.out.println("Usage : java Test1 \"n1\" \"op\" \"n2\" ");
			System.exit(-1);
		}
		double d1 = Double.parseDouble(args[0]);
		double d2 = Double.parseDouble(args[2]);
		double d = 0;
		if(args[1].equals("+")) {
			d = d1+d2;
		}
		else if(args[1].equals("-")) {
			d = d1-d2;
		}
		else if(args[1].equals("x")) {
			d = d1*d2;
		}
		System.out.println(d);
	}

}