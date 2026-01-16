package examples.threadExam;

public class ThreadExam01 {

	public static void main(String[] args) {
		MyThread01 m1   = new MyThread01("*");
		MyThread01 m2   = new MyThread01("-");
		MyThread01 m3   = new MyThread01("%");
		m1.start();
		m2.start();
		m3.start();
		
		System.out.println("Main메소드 종료");
		

	}

}


class MyThread01 extends Thread {
	private String str;
	public MyThread01 (String str) {
		this.str = str;
	}

	@Override
	public void run() {
		for(int i=0 ; i < 5 ;i++) {
			System.out.println(str);
			try {
				Thread.sleep((long)(Math.random() * 1000));
			}catch(InterruptedException ie) {}
		}
	}
	
}