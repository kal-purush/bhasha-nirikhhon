package EXe2;
import java.util.*;

public class MoneyConverter {
    String intStr;
    String fracStr;
    String num;
    Scanner in;
    float fl;
    boolean conZero;
    int flag =0;
    private static final String[] CN_NUMBERS = {"零", "壹", "贰", "叁", "肆", "伍", "陆", "柒", "捌", "玖"}; 
    private static final String[] CN_UNITS = {"圆", "拾", "佰", "仟"};  
    private static final String[] CN_BIG_UNITS = {"", "万", "亿", "兆"};  
    private static final String[] CN_Min_UNITs = {"分","角"};
    public void fun(){
        do {
            System.out.print("请输入金额: ");
            in = new Scanner(System.in);
            num = in.next();
            try {
                float fl = Float.parseFloat(num);
                if (fl < 0) {
                    System.out.println("金额不能小于0,请重新输入!");
                } else {
                    break;
                }
            } catch (NumberFormatException e) {
                System.out.println("非法输入,金额必须是数字: " );
            }
        } while (true); // 循环直到输入有效
        //先进行一下预处理，去除前导零（零按位置分，分为前导零，中间零和末尾零）
        num = num.replaceFirst("^0+", "");
        if(num.contains(".") == false){
            intStr = num;
            fracStr = null;
        }
        else{
            String []s = num.split("\\.");
            intStr = s[0];//整数部分
            //System.out.println("s[0]="+s[0]);
            fracStr = s[1];//小数部分
            //System.out.println("s[1]="+s[1]);
        }
        for(int i=0;i<intStr.length();i++){
            switch (intStr.charAt(i)) {
                case '0':
                //先假定是末尾连续零，再判断是不是末尾的连续零
                conZero = true;
                int j = i;
                for(;i<intStr.length();i++){//1000
                    if(intStr.charAt(i) != '0'){//如果从当前零开始一直到末尾不全为零
                        conZero = false;
                        break;
                    }  
                    if((intStr.length()-i == 5 || intStr.length()-i == 9) && flag == 0){
                        print1(intStr,i);//中途遍历到了万位或者亿位的零要打印数位
                        flag = 1;
                    }        
                }
                i = i -1;//保证跳出循环的i对应的字符是连续零的最后一个‘0’
                if(conZero){
                    System.out.print("圆整");
                    return;
                }
                //代码执行到这里，说明不是末尾连续零
                //同时，易推出此时i已经对应了最后一个连续零的最后一个零（只有一个零视为连续零的个数为1）
                if(intStr.length() - i == 5 || intStr.length() - i == 9){}//连续零如果以万位或者亿位为终止可以不打印零。而只打印数位，前面亦已经执行过
                else{System.out.print("零");}
                    break;
                case '1':
                    if((intStr.length()-i == 2 && intStr.length() == 2) || (intStr.length()-i == 6 && intStr.length() == 6)
                    || (intStr.length()-i == 10 && intStr.length() == 10)){ //十元，十万元，十亿元省略数字

                    }
                    else{
                        System.out.print(CN_NUMBERS[1]);
                    }  
                    print1(intStr, i);
                    break;
                case '2':
                    System.out.print(CN_NUMBERS[2]);
                    print1(intStr, i);
                    break;
                case '3':
                    System.out.print(CN_NUMBERS[3]);
                    print1(intStr, i);
                    break;
                case '4':
                    System.out.print(CN_NUMBERS[4]);
                    print1(intStr, i);
                    break;
                case '5':
                    System.out.print(CN_NUMBERS[5]);
                    print1(intStr, i);
                    break;
                case '6':
                    System.out.print(CN_NUMBERS[6]);
                    print1(intStr, i);
                    break;
                case '7':
                    System.out.print(CN_NUMBERS[7]);
                    print1(intStr, i);
                    break;
                case '8':
                    System.out.print(CN_NUMBERS[8]);
                    print1(intStr, i);
                    break;
                case '9':
                    System.out.print(CN_NUMBERS[9]);
                    print1(intStr, i);
                    break;
                case '.':
                    break;
                default:
                    System.out.print("Error");
                    break;
            }
        }
        if(fracStr != null){
            for(int i=0;i<fracStr.length();i++){
                switch (fracStr.charAt(i)) {
                    case '0':
                    System.out.print(CN_NUMBERS[0]);
                    break;
                case '1':
                    System.out.print(CN_NUMBERS[1]);
                    print2(fracStr, i);
                    break;
                case '2':
                    System.out.print(CN_NUMBERS[2]);
                    print2(fracStr, i);
                    break;
                case '3':
                    System.out.print(CN_NUMBERS[3]);
                    print2(fracStr, i);
                    break;
                case '4':
                    System.out.print(CN_NUMBERS[4]);
                    print2(fracStr, i);
                    break;
                case '5':
                    System.out.print(CN_NUMBERS[5]);
                    print2(fracStr, i);
                    break;
                case '6':
                    System.out.print(CN_NUMBERS[6]);
                    print2(fracStr, i);
                    break;
                case '7':
                    System.out.print(CN_NUMBERS[7]);
                    print2(fracStr, i);
                    break;
                case '8':
                    System.out.print(CN_NUMBERS[8]);
                    print2(fracStr, i);
                    break;
                case '9':
                    System.out.print(CN_NUMBERS[9]);
                    print2(fracStr, i);
                    break;
                default:
                    System.out.print("Error");
                    break;
                }
            }
        }
        in.close();
    }

    public void print1(String intstr,int i){
        switch (intStr.length()-i) {//intStr.length()-i表示处于从右往左第几位
            case 1:
                System.out.print(CN_UNITS[0]);
                if(fracStr == null){
                    System.out.print("整");//没有小数部分，打印整
                }
                break;
            case 2:
            case 6:
            case 10:
                System.out.print(CN_UNITS[1]);
                break;
            case 3:
            case 7:
            case 11:
                System.out.print(CN_UNITS[2]);
                break;
            case 4:
            case 8:
            case 12:
                System.out.print(CN_UNITS[3]);
                break;
            case 5:
                System.out.print(CN_BIG_UNITS[1]);
                break;
            case 9:
                System.out.print(CN_BIG_UNITS[2]);
                break;
            default:
                break;
        }
    }

    public void print2(String fracStr,int i){
        switch (fracStr.length()-i) {
            case 1:
                System.out.print(CN_Min_UNITs[0]);
                break;
            case 2:
                System.out.print(CN_Min_UNITs[1]);
            default:
                break;
        }
    }
    
}

class Main{
    public static void main(String[] args) {
        MoneyConverter moneyConverter = new MoneyConverter();
        moneyConverter.fun();
    }
}