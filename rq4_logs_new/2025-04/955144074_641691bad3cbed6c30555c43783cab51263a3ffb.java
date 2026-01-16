package begayim.recursion1;

public class rec_10_11_12 {


    public int countX(String str){
        if (str.isEmpty()){
            return 0;
        }
        if (str.charAt(0) == 'x'){
            return 1 + countX(str.substring(1));
        } else {
            return countX(str.substring(1));
        }
    }
    public String changeXY(String str){
        if (str.isEmpty()){
            return str;
        }
        char firstChar = str.charAt(0);
        if (firstChar == 'x'){
            return "y" + changeXY(str.substring(1));
        } else {
            return firstChar + changeXY(str.substring(1));
        }
    }

    public int countHi(String str){
        if (str.length() < 2){
            return 0;
        }
        if (str.substring(0,2).equals("hi")){
            return 1 + countHi(str.substring(2));
        }else {
            return countHi(str.substring(1));
        }
    }
}