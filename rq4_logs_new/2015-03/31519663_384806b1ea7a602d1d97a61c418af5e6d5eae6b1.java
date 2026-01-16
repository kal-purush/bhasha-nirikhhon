package thoughts;

import java.util.concurrent.Callable; 
import java.util.concurrent.ExecutorService;   
import java.util.concurrent.Executors;   
import java.util.concurrent.Future; 
import java.util.logging.Logger;
  
/*
 * Callable �� Future�ӿ�  
 * Callable��������Runnable�Ľӿ�,ʵ��Callable�ӿڵ����ʵ��Runnable���඼�ǿɱ������߳�ִ�е�����  
 * Callable��Runnable�м��㲻ͬ:  
 * (1)Callable�涨�ķ�����call(),��Runnable�涨�ķ�����run().  
 * (2)Callable������ִ�к�ɷ���ֵ(��������),��Runnable�������ǲ��ܷ���ֵ�ġ�  
 * (3)call()�������׳��쳣,��run()�����ǲ����׳��쳣�ġ�  
 * (4)����Callable������õ�һ��Future����,  
 * Future ��ʾ�첽����Ľ�������ṩ�˼������Ƿ���ɵķ���,�Եȴ���������,����������Ľ����  
 * ͨ��Future������˽�����ִ�����,��ȡ�������ִ��,���ɻ�ȡ����ִ�еĽ����  
 */  
public class CallableAndFuture {   
	
	private static final String TAG = "xlx";
  
    /** *//**  
     * �Զ���һ��������,ʵ��Callable�ӿ�  
     */  
    public static class MyCallableClass implements Callable{   
        // ��־λ   
        private int flag = 0;   
        public MyCallableClass(int flag){   
            this.flag = flag;   
        }
        
		@Override
        public String call() throws Exception{   
            if (this.flag == 0){
                // ���flag��ֵΪ0,����������   
                return "flag = 0";   
            }    
            if (this.flag == 1){
                // ���flag��ֵΪ1,��һ������ѭ��   
                try {   
                    while (true) {   
                        System.out.println("looping.");   
                        Thread.sleep(1000);   
                    }   
                } catch (InterruptedException e) {   
                    System.out.println("Interrupted");   
                }   
                return "false";   
            } else {   
                // falg��Ϊ0����1,���׳��쳣   
                throw new Exception("Bad flag value!");   
            }   
        }   
    }   
       
    @SuppressWarnings("unchecked")
	public static void main(String[] args) {   
        // ����3��Callable���͵�����   
        /*MyCallableClass task1 = new MyCallableClass(0);   
        MyCallableClass task2 = new MyCallableClass(1);   
        MyCallableClass task3 = new MyCallableClass(2);   
        // ����һ��ִ������ķ���   
        ExecutorService es = Executors.newFixedThreadPool(3);   
        try {   
            // �ύ��ִ������,��������ʱ������һ�� Future����,   
            // �����õ�����ִ�еĽ���������쳣�ɶ����Future������в���   
            Future future1 = es.submit(task1);   
            // ��õ�һ������Ľ��,�������get����,��ǰ�̻߳�ȴ�����ִ����Ϻ������ִ��   
            System.out.println("task1: " + future1.get());   
               
            Future future2 = es.submit(task2);   
            // �ȴ�5���,��ֹͣ�ڶ���������Ϊ�ڶ���������е�������ѭ��   
            Thread.sleep(5000);   
            //future2.cancel(true)�ᴥ��InterruptedException��
            System.out.println("task2 cancel: " + future2.cancel(true));   
               
            // ��ȡ��������������,��Ϊִ�е���������������쳣   
            // �����������佫�����쳣���׳�   
            Future future3 = es.submit(task3);   
            System.out.println("task3: " + future3.get());   
        } catch (Exception e){   
            System.out.println(e.toString());   
        }   
        // ֹͣ����ִ�з���   
        es.shutdownNow();   */
    	CvbsStateRefresh mThread = new CvbsStateRefresh();
    	mThread.start();
    	mThread.setCurrentState(CvbsStateRefresh.RUNNING);
    	try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	mThread.setCurrentState(CvbsStateRefresh.EXIT);
    }   
    
    private static class CvbsStateRefresh extends Thread {
      public static final int NO_START = 0;
      public static final int RUNNING = 1;
      public static final int PAUSE = 2;
      public static final int EXIT = 3;
      public int currentState = NO_START;
      public Object obj = new Object();
  
      public void setCurrentState(int state) {
         synchronized (obj) {
            this.currentState = state;
         }
      }
  
      public int getCurrentState() {
         synchronized (obj) {
            return this.currentState;
         }
      }
  
      @SuppressWarnings("static-access")
      private synchronized void doReadCvbsState() {
         
      }
  
      private synchronized void loopRun() {
         while (true) {
            if (getCurrentState() == NO_START) {
               try {
            	   System.out.println("NO_START...");
//                  wait(3000);
                  Thread.sleep(1000);
               } catch (Exception e) {
                  e.printStackTrace();
               }
            } else if (getCurrentState() == RUNNING) {
               doReadCvbsState();
               try {
            	   System.out.println("RUNNING...");
//                  wait(3000);
                  Thread.sleep(1000);
               } catch (InterruptedException e) {
                  // TODO Auto-generated catch block
                  e.printStackTrace();
               }
            } else if (getCurrentState() == PAUSE) {
               try {
            	   System.out.println("PAUSE...");
//                  wait(3000);
                  Thread.sleep(1000);
               } catch (Exception e) {
                  e.printStackTrace();
               }
            } else if (getCurrentState() == EXIT) {
//               Logger.getAnonymousLogger().info("stop this thread!");
            	System.out.println("stop this thread!");
               break;
            }
         }
      }
  
      public void run() {
         /*if (logOn) {
            Log.e(TAG, "start method mCVBSModule.cvbsStateRefresh!");
         }*/
         loopRun();
      }
   }

}