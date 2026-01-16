package ThreadsPractice;
class MyThread2 implements Runnable
{
	public void run()
	{
		DisplayEvenNumbers();
	}
	synchronized void DisplayEvenNumbers()
	{
		try
		{
			for(int i=20;i<=30;i++)
			{
				if(i%2==0)
				{
					Thread.sleep(1000);
					System.out.println(Thread.currentThread().getName()+"DisplayEvenNumbers:"+i);
				}
			}
		}catch(Exception e)
		{
			e.printStackTrace();
		}
	}
}
public class ThreadSafe {

	public static void main(String[] args) {
		MyThread2 aa=new MyThread2();
		Thread aa1=new Thread(aa);
		Thread aa2=new Thread(aa);
		aa1.setName("alpha");
		aa2.setName("omegha");
		aa1.start();
		aa2.start();
 
	}

}