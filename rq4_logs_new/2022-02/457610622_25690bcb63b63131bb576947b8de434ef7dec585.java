package com.example.myapplication.javatest;

import android.os.Handler;
import android.os.Looper;

/**
 * 字节码插桩测试
 */
public class ByteStubTest {
   /**
    * hdpi下chengxujieshuqi描述了程序计数器的意义，即cpu在执行程序时是时间片轮转的，将一个周期分割成多个时间片，每个
    * 时间片时间固定，多个线程抢占cpu时间片，当一个时间片内线程内程序执行不完，下次抢占到后需要从之前位置开始执行程序计数器
    * 的意义就是记录执行位置（记录程序运行的地址）
    *
    * 栈帧：hdpi下zhanzheng，即程序执行方法时开辟的内存空间，用于存放执行方法
    * 递归oom也是因为栈帧压入方法过多导致栈溢出
    */

   /**
    * Android回crash（崩溃）的原因：Android的main函数中（RuntimeInit类的main（）方法），调用了commonInit（）
    * 方法，该方法调用了Thread.setDefaultUncaughtExceptionHandler(new KillApplicationHandler(loggingHandler));
    * 方法，KillApplicationHandler中，杀死线程，弹出崩溃弹窗，杀死app
    *
    * 所以：当异常KillApplicationHandler捕获到异常，进行完一系列处理（主要是打印crash日志，通知AMS展示crash弹窗等）
    * 后，最终会杀死进程，这样你的app就崩溃了。
    *
    * android异常捕获机制：
    * Thread.setCaughtExceptionPreHandler()覆盖所有线程，会在回调DefaultExceptionHandler之前调用；
    * Thread.setCaughtExceptionHandler()同样回覆盖所有线程，可以在应用层被重复调用，并且每一次调用后，
    * 都会覆盖上一次设置的DefaultUncaughtExceptionHandler；
    * Thread.currentThread.setUncaughtExceptionHandler()，只可以覆盖当前线程的异常。如果某个线程存在自定义的
    * UncaughtExceptionHandler，回调时会忽略全局的DefaultUncaughtHandler。
    *
    * 要想不crash，只能让线程不要抛出exception，唯此别无他法。如果我们能把一个线程的所有的操作都使用try-catch进行保护
    * ，理论上，就能做到app never crash。由于android基于Handler事件驱动的机制，可以在app启动时，向主线程中的
    * MessageQueue中提交一个死循环操作，在这个死循环中不断去poll事件，并且将这个死循环进行try-catch，这样所有主线程中
    * 的异常都会被catch住，从而app就再也不会发生crash。
    */

   public void neverCrash(){
       Handler handler = new Handler();
       handler.post(new Runnable() {
           @Override
           public void run() {
               while (true){
                   try {
                       Looper.loop();
                   }catch (Exception e){
                   }
               }
           }
       });
   }

    /**
     * Android布局嵌套多少回crash？
     * 所有的view都是通过反射造出来的，所有自定义view的时候不能省略两个构造函数的构造方法
     * 看层级，内部有递归，如果层级太多，递归造成栈溢出就oom了
     */

    /**
     * 字节码执行方法
     * 方法执行时，先将操作数放入操作数栈，等执行放入局部变量表语句时，将栈顶的操作数出栈，写入局部变量表，然后遇到操作数
     * 如+，贼弹出操作数栈内两个数进行操作，在压入操作数栈，直至遇到放入语句，放入语句带下标，即放入局部变量表的位置，最后
     * 将局部变量表的数据到方法出口输出，动态链接的意思是，对于jvm来说，当多态方法时，jvm是不知道执行哪个方法的，就由动态
     * 链接去确定该执行哪个方法（也多用于本地方法的确定）
     */



}