package question1;

public class PolyDriver {

	public static void main(String[] args) {
		PolyStack poly = new PolyStack();
		
		poly.push(4, 0);
		poly.push(-2, 1);
		poly.push(5, 2);
		poly.push(-6,  3);
		
		System.out.println("Original Polynomial: ");
		poly.printPoly();
		System.out.println();
		
		PolyStack product = poly.multPoly();
		System.out.println("Polynomial multiplied by itself: ");
		product.printPoly();
		System.out.println();

		
		System.out.println("Original Polynomial: ");
		poly.printPoly();
		System.out.println();

		
		PolyStack sum = poly.addPoly();
		System.out.println("Polynomial added to iteself: ");
		sum.printPoly();
		System.out.println();
	}
	
}