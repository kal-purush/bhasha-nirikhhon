package constructor;

public class Cons1 {

		String names;
		int id;
		
		//public Cons1(String name,int no) {
		 //Cons1(String name,int no) {
		 //protected Cons1(String name,int no) {
		private Cons1(String name,int no) {
			names=name;
			id=no;
		}
		public void display() {
			System.out.println(names+" "+id);
		}
		public static void main(String[] args) {
			// TODO Auto-generated method stub
			Cons1 c1=new Cons1("vinod",25);
			Cons c2=new Cons("ravi",25);
			c1.display();
			c2.display();

		

	}

}