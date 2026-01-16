/*
George Lyu
Mr.Daniel
S3C7
This is a question of 2017 APCS Exam
 */
package digits;

/**
 *
 * @author lvzhaozhou
 */
public class Digits {

    private Arraylist<Integer> digitList;
    public Digits (int num){
        digitList = new Arraylist<Integer>();
        
        if (num == 0){
            digitList.add(new Integer(0));
        }
        if (num >=0){
            digitList.add(0, new Integer(num%10));
            num/=10;
        }
    }
    public boolean isStrictlyIncreasing(){
        for (int i=0;i<digitList.size()-1;i++){
            if(digitList.get(i+1)>=digitList.get(i)){
                return true;
            }
            else
                return false;
        }
    
    public static void main(String[] args) {
        // TODO code application logic here
    }
    
}