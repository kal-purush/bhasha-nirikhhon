public class ReverseBits {
    // you need treat n as an unsigned value
    public int reverseBits(int n) {

        int times=32;
        List<Integer> list=new ArrayList<>();
        while(times>0){
            int t=n&1;// to get the last digit
            list.add(t); //as we get last digit each time, our list is made in reverse order
            n=n>>1;  //remove that last digit
            times--;
        }
        //step-2 rebuild the number
        int res=0;
        for(int i=0;i<list.size();i++){
            res=res<<1;
            res=res | list.get(i);
        }
        return res;
    }
}