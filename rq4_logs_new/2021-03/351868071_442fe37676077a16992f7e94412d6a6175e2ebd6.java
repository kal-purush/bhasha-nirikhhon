package level1;

public class collatz {
    public static int solution(int num) {
        int answer = 0;
        long n = (long)num;
        while(n!=1){
            if(n%2 == 0) n/=2;
            else  n = n*3 + 1;

            if(answer>=500) return -1;
            answer++;
        }

        return answer;
    }

    public static void main(String [] args){
        System.out.print(solution(6));
    }
}
package level1;
import java.util.*;

public class k번째수 {

    public static int[] solution(int[] array, int[][] commands) {
        int[] answer = new int [commands.length];
        /*
        1 5 2 6 3 7 4

        2 5 3 // 5 2 6 3 // 2 3 5 6 // 5
        4 4 1 // 6 // 6 // 6
        1 7 3 // 1 5 2 6 3 7 4 // 1 2 3 4 5 6 7 // 3
        */
        int [][] tmp = new int[commands.length][commands[0].length];

        for(int i=0;i<commands.length;i++){
            int cnt = 0;
            for(int j=commands[i][0]-1;j<=commands[i][1]-1;j++){
                tmp[i][cnt] += array[j];
                cnt++;
            }
        }

        for(int k=0;k<commands.length;k++){
            Arrays.sort(tmp[k]);
        }

        for(int k=0;k<commands.length;k++){
            answer[k] = tmp[k][commands[k][2]-1];
        }

        return answer;
    }

    public static void main(String[] args) {
        //[1, 5, 2, 6, 3, 7, 4]	[[2, 5, 3], [4, 4, 1], [1, 7, 3]]
        // 답 5 6 3

        int[] array = {1, 5, 2, 6, 3, 7, 4};
        int[][] commands = { {2, 5, 3}, {4, 4, 1}, {1, 7, 3}};

        solution(array,commands);
    }

}
