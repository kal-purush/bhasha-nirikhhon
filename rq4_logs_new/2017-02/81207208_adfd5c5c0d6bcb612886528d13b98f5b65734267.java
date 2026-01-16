public class Proov1{
	public static void main(String[] args){
		Ristkylik laud1=new Ristkylik(2, 1);
		Ristkylik laud2=new Ristkylik(1.5, 1);
		Ristkylik laud3=new Ristkylik(2, 1.5);
		System.out.println(laud1.pindala()+" "+laud1.ymberm66t());
		System.out.println(laud2.pindala());
		System.out.println(laud3.pindala());
		System.out.println(laud1);
		System.out.println(laud2.toString());
	}
}