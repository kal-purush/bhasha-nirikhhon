package begayim.recursion1;

public class rec_13_14_15 {

    public String changePi (String str){
        if (str.length() < 2){
            return str;
        }
        if (str.startsWith("pi")){
            return "3.14" + changePi(str.substring(2));
        } else {
            return str.charAt(0) + changePi(str.substring(1));
        }
    }

    public String noX(String str){
        if (str.isEmpty()){
            return str;
        }
        if (str.charAt(0) == 'x'){
            return noX(str.substring(1));
        }else {
            return str.charAt(0) + noX(str.substring(1));
        }
    }

    public boolean array6(int[] nums, int index){
        if (index >= nums.length){
            return false;
        }
        if (nums[index] == 6){
            return true;
        }
        return array6(nums, index + 1);
    }
}