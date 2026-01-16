package practice.HIndex;
import java.util.*;

/*
    앞에서부터 서치하는 이유는 최댓값을 찾기는 하지만
    뒤로 밀릴수록 해당하는 원소의 개수가 그만큼 적어지니
    맨 처음의 조건에 만족하는 원소를 찾을 필요가 있다.
    h 이하의 원소 개수도 h 이하의 원소 개수 만큼 있어야 하니
    사실 최댓값이라는 말에 주목하지 않아도 되는 문제였다.
 */
class Solution3 {
    public int solution(int[] citations) {
        Arrays.sort(citations);
        int answer = 0;
        int n = citations.length;

        for(int i = 0; i < n; i++){
            int h = n-i;

            if(citations[i] >= h){
                answer = h;
                break;
            }
        }

        return answer;
    }
}