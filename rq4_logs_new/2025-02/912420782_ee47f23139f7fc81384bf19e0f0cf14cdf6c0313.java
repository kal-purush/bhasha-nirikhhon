public class bill {

    class Solution {
    public int solution(int[] wallet, int[] bill) {
        int answer = 0;
        int h = Math.min(wallet[0],wallet[1]);
        int w= Math.max(wallet[0],wallet[1]);
        int bH = Math.min(bill[0],bill[1]);
        int bW = Math.max(bill[0],bill[1]);

        while(h < bH || w < bW){
            bW /= 2;
            answer++;

            if(bW < bH){
                int temp = bW;
                bW = bH;
                bH = temp;
            }

            if(bW <= w && bH <= h){
                break;
            }
        }

        return answer;
    }
}
}