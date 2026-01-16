public class Cat implements Runnable,Jumpable{

    int runLength;
    int jumpLength;
    String name;

    public Cat(String name,int runLength, int jumpLength) {
        this.runLength = runLength;
        this.jumpLength = jumpLength;
        this.name = name;

    }
    @Override
    public void run(int i) {
        if(runLength < i){
            System.out.println(name + "cant run so far");
        }else{
            System.out.println(name + " run " + runLength);
        }
    }

    @Override
    public void jump(int i) {
        if(jumpLength > i){
            System.out.println(name + "cant jump so far");
        }else{
            System.out.println(name + " jump " + jumpLength + " wall");
        }
    }
}