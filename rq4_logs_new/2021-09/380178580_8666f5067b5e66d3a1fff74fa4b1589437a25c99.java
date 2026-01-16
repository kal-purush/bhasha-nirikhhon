public class ChainTest {

	private int num;
	
	public ChainTest() {
		this.num = 10;
	}
	
	public int getNum() {
		return this.num;
	}
	
	public int plusNum() {
		return ++this.num;
	}
	
	public void showNum() {
		System.out.println("num값음 : " + num);
	}
	
	public ChainTest plusThis() {
		this.num++;
		return this;
	}
	
}