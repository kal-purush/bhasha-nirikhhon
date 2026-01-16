class Here{



    synchronized void printTable(int n){
        for(int i=1;i<=10;i++){
            System.out.println(n*i);
            try{

                Thread.sleep(400);
            }catch(Exception e){}
        }
    }
}

class MyThread1 extends Thread{
    public void run(){
        Here h = new Here();
        h.printTable(1);
        System.out.println("end 1");
    }
}

class MyThread2 extends Thread{
    public void run(){
        Here h = new Here();
        h.printTable(10);
        System.out.println("end 2");
    }
}

class MyThread3 extends Thread{
    public void run(){
        Here h = new Here();
        h.printTable(100);
        System.out.println("end 3");
    }
}




class MyThread4 extends Thread{
    public void run(){
        Here h = new Here();
        h.printTable(1000);
        System.out.println("end 4");
    }
}

class TestSynchronization4{
    public static void main(String t[]){
        MyThread1 t1=new MyThread1();
        MyThread2 t2=new MyThread2();
        MyThread3 t3=new MyThread3();
        MyThread4 t4=new MyThread4();
        t1.start();
        t2.start();
        t3.start();
        t4.start();
    }
}
