
public class Caller {
	interface CallBack {
		void methodToCallBack() throws InterruptedException;
	}
	public void registerBack(CallBack callBack) throws InterruptedException {
		callBack.methodToCallBack();
	}
	public void register() throws InterruptedException {
		Thread.sleep(3000);
		System.out.println("gaga");
	}
	
	public static void main(String[] args) throws InterruptedException {
		Caller caller = new Caller();
		caller.register();
		caller.register();
		caller.registerBack(new CallBack() {

			@Override
			public void methodToCallBack() throws InterruptedException {
				// TODO Auto-generated method stub
				Thread.sleep(3000);
				System.out.println("haha");
			}
			
		});
		caller.registerBack(new CallBack() {

			@Override
			public void methodToCallBack() throws InterruptedException {
				// TODO Auto-generated method stub
				Thread.sleep(3000);
				System.out.println("haha");
			}
			
		});
		caller.registerBack(new CallBack() {

			@Override
			public void methodToCallBack() throws InterruptedException {
				// TODO Auto-generated method stub
				Thread.sleep(3000);
				System.out.println("haha");
			}
			
		});
	}
}