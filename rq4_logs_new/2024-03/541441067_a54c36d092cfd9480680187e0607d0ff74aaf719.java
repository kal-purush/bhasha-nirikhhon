import java.util.*;

class Solution {
    ArrayList<Integer>[] linkedList;
    boolean[] isVisited;
    int result;
    public int solution(int n, int[][] wires) {
        linkedList = new ArrayList[n+1];
        isVisited = new boolean[n+1];
        result = n;
        init(n, wires);
        dfs(n, 1);

        return result;
    }
    private void init(int n, int[][] wires){
        //초기화
        for(int i=1; i<=n; i++){
            linkedList[i] = new ArrayList<>();
        }

        for(int i=0; i<n-1; i++){
            int x= wires[i][0], y=wires[i][1];
            linkedList[x].add(y);
            linkedList[y].add(x);
        }
    }
    private int dfs(int total, int currNode){
        isVisited[currNode] = true;
        //현재 노드의 합계
        int currSum = 1;
        for(int i=0; i<linkedList[currNode].size(); i++){
            int nextNode = linkedList[currNode].get(i);
            if(!isVisited[nextNode]){
                currSum += dfs(total, nextNode);
            }
        }
        int remain = total - currSum;
        result = (result > (Math.abs(remain-currSum))) ? Math.abs(remain-currSum) : result;
        return currSum;
    }
}