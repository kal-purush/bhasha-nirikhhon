package _0205._1213;

public class Test {
    public static void main(String[] args) {

        String s = "aabaa";

        int n = s.length();
        for(int i = 0; i < n/2 ; i++){
            if(s.charAt(i) == s.charAt(n - i - 1)){
                System.out.println(s.charAt(i));

                if(i == n/2 - 1){
                    System.out.println("here");
                    System.out.println(s);
                }
            }
        }

    }
}
package _0206._16926;

import java.util.Scanner;


//   // 배열을 돌릴때 1 2 3
//            //            4 5 6
//
//            // 이면 왼쪽 위에 꺼 빼놓고
//            // 2 3 을 왼쪽으로 한칸씩
//            // 6을 위로 밀고
//            // 45 를 오른쪽으로 밀고
//            // 왼쪽 위에 있던걸 아래에 넣기
public class Main_16926_배열돌리기1_문영호 {
    static int N;
    static int M;
    static int R;
    static int[][] arr;

    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);

        N = sc.nextInt();
        M = sc.nextInt();
        R = sc.nextInt();
        arr = new int[N][M];

        for (int i = 0; i < N; i++) {
            for (int j = 0; j < M; j++) {
                arr[i][j] = sc.nextInt();
            }
        }


        for(int i = 0; i < R; i++){
            rotateArr();
        }
//        rotateArr();

        printArr();

    }

    static void rotateArr() {

        int left = 0;
        int right = M - 1;
        int top = 0;
        int bottom = N - 1;

        int c = Math.min(N, M) / 2;// 이게 진짜 조건에 min(N, M) mod 2 = 0
        // -> N, M 중 작은것의 /2 만큼 돌릴 수 있음 돌려보면 됨

        for(int i = 0; i < c; i++){

            // 배열을 돌릴때 1 2 3
            //            4 5 6

            // 이면 왼쪽 위에 꺼 빼놓고
            // 2 3 을 왼쪽으로 한칸씩
            // 6을 위로 밀고
            // 45 를 오른쪽으로 밀고
            // 왼쪽 위에 있던걸 아래에 넣기
            int temp = arr[i][i];

//            System.out.println("first");
//            printArr();
            for(int j = left; j < right ; j++){
                arr[top][j] = arr[top][j + 1];
            }
            top++;
//            System.out.println("after 1");

//            printArr();

            for(int j = top; j <= bottom; j++){
                arr[j - 1][right] = arr[j][right];
            }
            right--;
//            System.out.println("after 2");

//            printArr();

            for(int j = right; j >= left; j--){
                arr[bottom][j+1] = arr[bottom][j];
            }
            bottom--;
//            System.out.println("after 3");

//            printArr();

            for(int j = bottom; j >= top; j--){
                arr[j +1][left] = arr[j][left];
            }
            left++;

//            System.out.println("after 4");
//            printArr();


            arr[i+1][i] = temp;
        }


    }

    static void printArr() {
//        System.out.println("----- start print arr----- ");
        for (int[] aa : arr) {
            for (int a : aa) {
                System.out.print(a + " ");
            }
            System.out.println();
        }
//        System.out.println("----- end print arr----- ");

    }
}
//4 4 2
//1 2 3 4
//5 6 7 8
//9 10 11 12
//13 14 15 16

/**


 // input
 4 4 2
 1 2 3 4
 5 6 7 8
 9 10 11 12
 13 14 15 16
 = 2
 5 4 7
 1 2 3 4
 7 8 9 10
 13 14 15 16
 19 20 21 22
 25 26 27 28
 =3
 2 2 3
 1 1
 1 1
 =4
 2 2 3
 1 2
 3 4
 =5
 5 2 5
 2 2
 1 3
 1 3
 1 3
 4 4
 = 6
 2 4 3
 1 2 3 4
 5 6 7 8
 =
 4 2 3
 1 2
 3 4
 5 6
 7 8
 =
 9 4 7
 1 1 1 1
 2 2 2 2
 3 3 3 3
 4 4 4 4
 5 5 5 5
 6 6 6 6
 7 7 7 7
 8 8 8 8
 9 9 9 9
 =
 5 4 6
 1 1 1 1
 2 2 2 2
 3 3 3 3
 4 4 4 4
 5 5 5 5
 =
 4 9 7
 1 2 3 4 5 6 7 8 9
 1 2 3 4 5 6 7 8 9
 1 2 3 4 5 6 7 8 9
 1 2 3 4 5 6 7 8 9
 =
 6 9 7
 1 2 3 4 5 6 7 8 9
 1 2 3 4 5 6 7 8 9
 1 2 3 4 5 6 7 8 9
 1 2 3 4 5 6 7 8 9
 1 2 3 4 5 6 7 8 9
 1 2 3 4 5 6 7 8 9

 // output
 1)
 3 4 8 12
 2 11 10 16
 1 7 6 15
 5 9 13 14

 2)
 28 27 26 25
 22 9 15 19
 16 8 21 13
 10 14 20 7
 4 3 2 1

 3)
 1 1
 1 1

 4)
 3 1
 4 2

 5)
 4 4
 3 1
 3 1
 3 1
 2 2

 6)
 4 8 7 6
 3 2 1 5

 7)
 6 8
 4 7
 2 5
 1 3

 8)
 5 6 7 8
 4 8 8 9
 3 7 7 9
 2 6 6 9
 1 5 5 9
 1 4 4 8
 1 3 3 7
 1 2 2 6
 2 3 4 5

 9)
 4 5 5 5
 3 2 2 5
 2 3 3 4
 1 4 4 3
 1 1 1 2

 10)
 8 9 9 9 9 8 7 6 5
 7 8 7 6 5 4 3 2 4
 6 8 7 6 5 4 3 2 3
 5 4 3 2 1 1 1 1 2

 11)
 8 9 9 9 9 9 9 8 7
 7 8 8 8 7 6 5 4 6
 6 8 5 4 3 3 4 3 5
 5 7 6 7 7 6 5 2 4
 4 6 5 4 3 2 2 2 3
 3 2 1 1 1 1 1 1 2
 */
package _0206._2615;

import java.security.Principal;
import java.util.Scanner;

// 오목
// 6개 연속은 X
// 5개 체크하고 넘어가면 6개 되는지 체크하기

// 가로 세로 대각선 2개 체크
// 슛

public class Main_2615_오목_문영호 {
    static int[][] arr;
    static int winner; // 검 1 흰 2
    static int winnerX;
    static int winnerY;
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);

        arr = new int[19][19];

        for(int i = 0; i < 19; i++){
            for(int j = 0; j < 19; j++){
                arr[i][j] = sc.nextInt();
            }
        }

        for(int i = 0; i < 19; i++){
            for(int j = 0; j < 19; j++){
                if(checkWinner(i, j)){
                    System.out.println(winner);
                    System.out.println(winnerX + " " + winnerY);
                    return;
                }
            }
        }

        System.out.println(0);



    }

    static boolean checkWinner(int x, int y) {
        int temp = arr[x][y];

        if (temp == 0)
            return false;

        if (checkWinner1(x, y, temp) || checkWinner2(x, y, temp) || checkWinner3(x, y, temp)
                || checkWinner4(x, y, temp)) {
            winner = temp;
            winnerX = x + 1;
            winnerY = y + 1;
            return true;
        }


        return false;
    }
        static boolean checkWinner1(int x, int y, int temp){
        for(int i = 1; i < 5; i++){
            int nx = x + i;
            // 세로
            if(!isIn(nx, y) || temp != arr[nx][y] )
                return false;
        }


        if(isIn(x + 5, y) && arr[x + 5][y] == temp){
            return false;
        }

        if(isIn(x - 1, y) && arr[x - 1][y] == temp){
            return false;
        }

        return true;
    }



        static boolean checkWinner2(int x, int y, int temp){

        for(int i = 1; i < 5; i++){
            int ny = y + i;
            if(!isIn(x, ny) || temp != arr[x][ny])
                return false;
        }
        // 1 1 1 1 1 1
        //   1 2 3 4 5
        if(isIn(x, y + 5) && temp == arr[x][y + 5]) {
            return false;
        }

        if(isIn(x, y -1) && temp == arr[x][y - 1]){
            return false;
        }

        return true;

    }

        static boolean checkWinner3(int x, int y, int temp){
        for(int i = 1; i < 5; i++){
            int nx = x + i;
            int ny = y + i;

            if(!isIn(nx, ny) || temp != arr[nx][ny ])
                return false;
        }

        if(isIn(x + 5, y + 5) && arr[x + 5][y + 5] == temp){
            return false;
        }

        if(isIn(x - 1, y - 1) && arr[x - 1][y - 1] == temp){
            return false;
        }

        return true;
    }





    static boolean checkWinner4(int x, int y, int temp){
        for(int i = 1; i < 5; i++){
            int nx = x - i;
            int ny = y + i;

            if(!isIn(nx, ny) || temp != arr[nx][ny ])
                return false;
        }

        if(isIn(x -5, y + 5) && arr[x -5][y + 5] == temp){
            return false;
        }

        if(isIn(x + 1, y -1 ) && arr[x + 1][y -1 ] == temp){
            return false;
        }

        return true;
    }
    static boolean isIn(int x, int y){
        return x >= 0 && x < 19 && y >=0 && y < 19;
    }
}

//        2 2 2 2 2 2 0 0 0 0 0 0 0 0 0 0 0 0 0
//        0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
//        0 0 2 0 0 2 2 2 1 0 0 0 0 0 0 0 0 0 0
//        0 0 1 2 0 0 0 0 1 0 0 0 0 0 0 0 0 0 0
//        0 0 0 1 2 0 0 0 0 0 0 0 0 0 0 0 0 0 0
//        0 0 0 0 1 2 2 0 0 0 0 0 0 0 0 0 0 0 0
//        0 0 1 1 0 1 0 0 0 0 0 0 1 0 0 0 0 0 0
//        0 0 0 0 0 0 2 1 0 0 0 1 0 0 0 0 0 0 0
//        0 0 0 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0 0
//        0 0 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0 0 0
//        0 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0 0 0 0
//        0 0 0 0 0 0 0 1 0 0 0 0 0 0 0 0 0 0 0
//        0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
//        0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
//        0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
//        0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
//        0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
//        0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
//        0 0 0 0 0 0 0 0 0 0 0 0 0 2 2 2 2 2 2