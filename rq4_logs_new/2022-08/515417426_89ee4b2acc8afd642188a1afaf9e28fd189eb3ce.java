package Level_1;

public class Lotto {
    static int[][] arr;
    public int[] solution(int rows, int columns, int[][] queries) {
        arr = new int[rows][columns];
        for (int i=0; i<rows; i++) {
            for (int j=0; j<columns; j++) {
                arr[i][j] = columns*i+j+1;
            }
        }

        int[] ans = new int[queries.length];
        for (int i=0; i<queries.length; i++) {
            ans[i] = rotate(queries[i][0], queries[i][1], queries[i][2], queries[i][3]);

        }
        return ans;
    }
    private int rotate(int r1, int c1, int r2, int c2) {
        r1--;
        c1--;
        r2--;
        c2--;

        int tmp=arr[r1][c1];
        int min =tmp;

        //왼쪽변 아래 -> 위
        for (int i=r1; i<r2; i++) {
            arr[i][c1] = arr[i+1][c1];
            min = Math.min(min, arr[i][c1]);
        }

        //아랫변 오른->왼
        for (int i=c1; i<c2; i++) {
            arr[r2][i] = arr[r2][i+1];
            min = Math.min(min, arr[r2][i]);
        }

        //오른쪽변 위->아래
        for (int i=r2; i>r1; i--) {
            arr[i][c2] = arr[i-1][c2];
            min = Math.min(min, arr[i][c2]);
        }

        //윗변 왼->오른
        for (int i=c2; i>c1; i--) {
            arr[r1][i] = arr[r1][i-1];
            min = Math.min(min, arr[r1][i]);
        }
        arr[r1][c1+1]=tmp;
        return min;
    }

}