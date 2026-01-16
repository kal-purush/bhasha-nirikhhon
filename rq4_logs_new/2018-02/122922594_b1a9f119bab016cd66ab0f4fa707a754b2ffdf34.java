import java.util.Scanner;
class CreateArray
{
	public static void main(String[] args) {
		Scanner scr=new Scanner(System.in);
		System.out.println("enter the array size");
		int n1=scr.nextInt();
		int[] a=new int[n];
		for(int i=0; i<a.length; i++) {
			System.out.println("enter the elements");
			a[i]=scr.nextInt();
		}
			System.out.println("array\n");
		for(int i=0; i<a.length; i++) {
			System.out.print(a[i] + " ");
		}
		System.out.println("enter the second array size");
		int n2=scr.nextInt();
		int[] b=new int[n1];
		for(int i=0; i<b.length; i++) {
			System.out.println("enter the elements");
			b[i]=scr.nextInt();
		}
			System.out.println("2nd array\n");
		for(int i=0; i<b.length; i++) {
			System.out.print(b[i] + " ");
		}	
		int[] c=new int[n1+n2];
		for(int i=0; i<a.length; i++) {
			c[i]=a[i];
		}
		int k=0;
		for(int j=a.length; j<c.length; j++) {
			c[j]=b[k++];
			}
		
			System.out.println("array\n");
		for(int i=0; i<c.length; i++) {
			System.out.print(c[i] + " ");
		}
}
}