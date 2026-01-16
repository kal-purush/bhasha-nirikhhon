public class IsInsideTest {
    public double Crossproduct(double a,double b,double c,double d){  //向量叉乘
        return a*d-b*c;
    }
    public  boolean isInside(double x, double y, double x1, double y1, double x2, double y2, double x3, double y3){
        //保证点是逆时针输入
        if(Crossproduct(x3-x1,y3-y1,x2-x1,y2-y1)>=0){
         double t=x2;
         double t2=y2;                //将其调整为逆时针
            x2=x3;
            y2=y3;
            x3=t;
            y3=t2;
        }
        if(Crossproduct(x2-x1,y2-y1,x-x1,y-y1)<0){
            return false;
        }
        if(Crossproduct(x1-x3,y1-y3,x-x3,y-y3)<0){
            return false;
        }
        if(Crossproduct(x3-x2,y3-y2,x-x2,y-y2)<0){
            return false;
        }
        return true;
    }

    public static void main(String[] args) {
        IsInsideTest test=new IsInsideTest();
        double x=1.2,y=1.5;
        double x1=4.5,y1=4.5;
        double x2=0.8,y2=1.1;
        double x3=6.2,y3=1.3;
        System.out.println(test.isInside(x,y,x1,y1,x2,y2,x3,y3));
    }
}
//顺时针也可以